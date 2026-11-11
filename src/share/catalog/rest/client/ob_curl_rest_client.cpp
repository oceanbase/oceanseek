/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/catalog/rest/client/ob_curl_rest_client.h"
#include "lib/string/ob_string.h"
#include "share/catalog/ob_catalog_properties.h"
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>
#include <openssl/err.h>

namespace oceanbase
{
namespace share
{

struct ObCurlRestClient::WriteContext
{
  WriteContext(ObSqlString &body, const int64_t max_response_body_size)
      : body_(body), max_response_body_size_(max_response_body_size), size_overflow_(false)
  {
  }

  ObSqlString &body_;
  const int64_t max_response_body_size_;
  bool size_overflow_;
};

static CURLcode ssl_ctx_callback(CURL *curl, void *ssl_ctx, void *userptr)
{
  UNUSED(curl);
  CURLcode ret = CURLE_OK;
  SSL_CTX *ctx = static_cast<SSL_CTX *>(ssl_ctx);
  const char *ca_file = static_cast<const char *>(userptr);

  if (OB_ISNULL(ctx)) {
    ret = CURLE_SSL_CERTPROBLEM;
    LOG_WARN("ssl context is null");
  } else if (OB_NOT_NULL(ca_file) && strlen(ca_file) > 0) {
    X509_STORE *store = SSL_CTX_get_cert_store(ctx);
    if (OB_ISNULL(store)) {
      ret = CURLE_SSL_CERTPROBLEM;
      LOG_WARN("failed to get cert store from ssl context");
    } else {
      if (1 != X509_STORE_load_locations(store, ca_file, NULL)) {
        ret = CURLE_SSL_CACERT_BADFILE;
        LOG_WARN("failed to load ca certificates from file", K(ca_file));
      }
    }
  }
  return ret;
}

int ObCurlRestClient::do_init(const ObRestCatalogProperties &properties)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("curl client already initialized");
  } else {
    curl_ = curl_easy_init();
    if (nullptr == curl_) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to init curl easy handle");
    } else {
      curl_easy_setopt(curl_, CURLOPT_NOSIGNAL, 1L);
      curl_easy_setopt(curl_, CURLOPT_TIMEOUT, properties.http_timeout_/1000/1000);
      curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);
      curl_easy_setopt(curl_, CURLOPT_MAXREDIRS, 3L);

      // TCP keepalive
      curl_easy_setopt(curl_, CURLOPT_TCP_KEEPALIVE, 1L);
      curl_easy_setopt(curl_, CURLOPT_TCP_KEEPIDLE, properties.http_keep_alive_time_/1000/1000);

      // SSL证书验证
      if (uri_.prefix_match(ObRestCatalogProperties::HTTPS_PREFIX)) {
        curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
        curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);
        curl_easy_setopt(curl_, CURLOPT_SSL_CTX_FUNCTION, ssl_ctx_callback);
        curl_easy_setopt(curl_, CURLOPT_SSL_CTX_DATA, OB_SSL_REST_CA_FILE);
      }
    }
  }
  return ret;
}

int ObCurlRestClient::do_destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    cleanup_curl();
  } else {
    ret = OB_NOT_INIT;
    LOG_WARN("curl client not initialized", K(ret));
  }
  return ret;
}

int ObCurlRestClient::do_reuse()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObCurlRestClient::execute(const ObRestHttpRequest &request, ObRestHttpResponse &response)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(request, response, 0))) {
    LOG_WARN("failed to execute REST request", KR(ret), K(request.get_url()));
  }
  return ret;
}

int ObCurlRestClient::execute(const ObRestHttpRequest &request,
                              ObRestHttpResponse &response,
                              const int64_t max_response_body_size)
{
  int ret = OB_SUCCESS;
  WriteContext write_context(response.body_, max_response_body_size);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("curl client not initialized", K(ret));
  } else if (OB_FAIL(set_curl_options(request, write_context))) {
    LOG_WARN("failed to set curl options", K(ret));
  } else {
    CURLcode cc = CURLE_OK;
    long http_code = 0;
    char *content_type = nullptr;
    if (CURLE_OK != (cc = curl_easy_perform(curl_))) {
      if ((CURLE_FILESIZE_EXCEEDED == cc && max_response_body_size > 0)
          || (CURLE_WRITE_ERROR == cc && write_context.size_overflow_)) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        ret = OB_CURL_ERROR;
      }
      LOG_WARN("curl perform failed", K(ret), K(cc),K(request.get_url()));
    } else if (CURLE_OK != (cc = curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_code))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("curl get info failed", K(ret), K(cc), K(request.get_url()));
    } else if (FALSE_IT(response.set_status_code(http_code))) {
      // do nothing
    } else if (http_code / 100 != 2 && http_code != 304) {  // 内部先校验状态码, 外部有需要可以重置状态码
      if (http_code == 401 || http_code == 403) {
        ret = OB_HTTP_UNAUTHORIZED;
        LOG_WARN("http unauthorized", K(ret), K(http_code), K(request.get_url()));
      } else if (http_code == 404) {
        ret = OB_HTTP_NOT_FOUND;
        LOG_WARN("http not found", K(ret), K(http_code), K(request.get_url()));
        LOG_USER_ERROR(OB_HTTP_NOT_FOUND, response.get_body().ptr());
      } else if (http_code / 100 == 4) {
        ret = OB_HTTP_BAD_REQUEST;
        LOG_WARN("http bad request", K(ret), K(http_code), K(request.get_url()));
        LOG_USER_ERROR(OB_HTTP_BAD_REQUEST, response.get_body().ptr());
      } else if (http_code / 100 == 5) {
        ret = OB_HTTP_SERVER_ERROR;
        LOG_WARN("http server error", K(ret), K(http_code), K(request.get_url()));
        LOG_USER_ERROR(OB_HTTP_SERVER_ERROR, response.get_body().ptr());
      } else {
        ret = OB_CURL_ERROR;
        LOG_WARN("http error", K(ret), K(http_code), K(request.get_url()));
      }
    } else if (CURLE_OK != (cc = curl_easy_getinfo(curl_, CURLINFO_CONTENT_TYPE, &content_type))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("curl get content type failed", K(ret), K(cc), K(request.get_url()));
    } else if (FALSE_IT(response.set_content_type(ObString(content_type)))) {
      // do nothing
    } else if (content_type != nullptr) {
      response.set_content_type(ObString(content_type));
    }
  }
  return ret;
}

int ObCurlRestClient::init_curl()
{
  int ret = OB_SUCCESS;
  
  return ret;
}

void ObCurlRestClient::cleanup_curl()
{
  if (nullptr != curl_) {
    curl_easy_cleanup(curl_);
    curl_ = nullptr;
  }
  if (nullptr != header_list_) {
    curl_slist_free_all(header_list_);
    header_list_ = nullptr;
  }
}

int ObCurlRestClient::set_curl_options(const ObRestHttpRequest &request,
                                       WriteContext &write_context)
{
  int ret = OB_SUCCESS;
  if (nullptr == curl_) {
    ret = OB_NOT_INIT;
    LOG_WARN("curl handle not initialized", K(ret));
  } else {
    CURLcode cc = CURLE_OK;
    ObString req_url;
    ObSqlString concat_url;
    const ObString &base_url = request.get_base_url().empty() ? uri_ : request.get_base_url();

    // url
    if (OB_FAIL(request.get_concat_url(req_url))) {
      LOG_WARN("failed to get concat url", K(ret));
    } else if (OB_FAIL(concat_url.append(base_url))) {
      LOG_WARN("failed to append uri", K(ret));
    } else if (OB_FAIL(concat_url.append(req_url))) {
      LOG_WARN("failed to append req url", K(ret));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_URL, concat_url.ptr()))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set URL", K(ret), K(cc), K(concat_url));
    }

    if (OB_SUCC(ret) && !request.get_base_url().empty()
        && base_url.prefix_match(ObRestCatalogProperties::HTTPS_PREFIX)) {
      if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L))
          || CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L))
          || CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_SSL_CTX_FUNCTION, ssl_ctx_callback))
          || CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_SSL_CTX_DATA, OB_SSL_REST_CA_FILE))) {
        ret = OB_CURL_ERROR;
        LOG_WARN("failed to configure request HTTPS verification", K(ret), K(cc));
      }
    }

    // Apply request-scoped transport options. Non-positive timeout values
    // leave the current Curl settings unchanged.
    if (OB_SUCC(ret) && !request.get_base_url().empty()) {
      const int64_t connect_timeout_ms = request.get_connect_timeout_ms();
      const int64_t request_timeout_ms = request.get_request_timeout_ms();
      const int64_t follow_redirects = request.get_follow_redirects() ? 1L : 0L;
      if (connect_timeout_ms > 0
          && CURLE_OK != (cc = curl_easy_setopt(curl_,
                                                CURLOPT_CONNECTTIMEOUT_MS,
                                                connect_timeout_ms))) {
        ret = OB_CURL_ERROR;
        LOG_WARN("failed to set connect timeout", K(ret), K(cc), K(connect_timeout_ms));
      } else if (request_timeout_ms > 0
                 && CURLE_OK != (cc = curl_easy_setopt(curl_,
                                                       CURLOPT_TIMEOUT_MS,
                                                       request_timeout_ms))) {
        ret = OB_CURL_ERROR;
        LOG_WARN("failed to set request timeout", K(ret), K(cc), K(request_timeout_ms));
      } else if (CURLE_OK != (cc = curl_easy_setopt(curl_,
                                                    CURLOPT_FOLLOWLOCATION,
                                                    follow_redirects))) {
        ret = OB_CURL_ERROR;
        LOG_WARN("failed to set redirect policy", K(ret), K(cc), K(follow_redirects));
      }
    }

    if (OB_SUCC(ret)) {
      if (CURLE_OK
          != (cc = curl_easy_setopt(curl_,
                                    CURLOPT_MAXFILESIZE_LARGE,
                                    static_cast<curl_off_t>(write_context.max_response_body_size_)))) {
        ret = OB_CURL_ERROR;
        LOG_WARN("failed to set maximum response body size",
                 K(ret),
                 K(cc),
                 K(write_context.max_response_body_size_),
                 K(request.get_url()));
      }
    }

    // http method
    if (OB_SUCC(ret)) {
      ObRestHttpMethod method = request.get_method();
      switch (method) {
        case HTTP_METHOD_HEAD: {
          if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_NOBODY, 1L))) {
            ret = OB_CURL_ERROR;
            LOG_WARN("failed to set NOBODY", K(ret), K(cc), K(concat_url));
          }
          break;
        }
        case HTTP_METHOD_POST: {
          if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_POST, 1L))) {
            ret = OB_CURL_ERROR;
            LOG_WARN("failed to set POST", K(ret), K(cc), K(concat_url));
          }
          break;
        }
        case HTTP_METHOD_PUT: {
          if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "PUT"))) {
            ret = OB_CURL_ERROR;
            LOG_WARN("failed to set CUSTOMREQUEST", K(ret), K(cc), K(concat_url));
          }
          break;
        }
        case HTTP_METHOD_DELETE: {
          if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "DELETE"))) {
            ret = OB_CURL_ERROR;
            LOG_WARN("failed to set CUSTOMREQUEST", K(ret), K(cc), K(concat_url));
          }
          break;
        }
        case HTTP_METHOD_GET: {
          if (!request.get_base_url().empty()) {
            if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, nullptr))) {
              ret = OB_CURL_ERROR;
              LOG_WARN("failed to reset CUSTOMREQUEST", K(ret), K(cc), K(concat_url));
            } else if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_HTTPGET, 1L))) {
              ret = OB_CURL_ERROR;
              LOG_WARN("failed to set HTTPGET", K(ret), K(cc), K(concat_url));
            }
          }
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument for http method", K(ret), K(method));
          break;
        }
      }
    }

    // body
    ObString body = request.get_body();
    // Empty POST requests must replace the body retained by a reused Curl handle.
    if (OB_SUCC(ret) && (!body.empty() || HTTP_METHOD_POST == request.get_method())) {
      if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, body.empty() ? "" : body.ptr()))) {
        ret = OB_CURL_ERROR;
        LOG_WARN("failed to set POSTFIELDS", K(ret), K(cc), K(concat_url));
      } else if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, body.length()))) {
        ret = OB_CURL_ERROR;
        LOG_WARN("failed to set POSTFIELDSIZE", K(ret), K(cc), K(concat_url));
      }
    }
    
    // headers
    common::ObArray<common::ObString> header_names;
    common::ObArray<common::ObString> header_values;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(request.get_all_headers(header_names, header_values))) {
      LOG_WARN("failed to get all headers", K(ret));
    } else if (header_names.count() != header_values.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("header names and values count mismatch", K(ret), K(header_names.count()), K(header_values.count()));
    } else {
      // Clean up previous header list if exists
      if (nullptr != header_list_) {
        curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, nullptr);
        curl_slist_free_all(header_list_);
        header_list_ = nullptr;
      }
      // Build header list for curl
      for (int64_t i = 0; i < header_names.count() && OB_SUCC(ret); ++i) {
        ObSqlString header_str;
        if (OB_FAIL(header_str.append_fmt("%.*s: %.*s", 
                                          header_names[i].length(), header_names[i].ptr(),
                                          header_values[i].length(), header_values[i].ptr()))) {
          LOG_WARN("failed to format header", K(ret), K(header_names[i]), K(header_values[i]));
        } else {
          struct curl_slist *new_header = curl_slist_append(header_list_, header_str.ptr());
          if (nullptr == new_header) {
            ret = OB_CURL_ERROR;
            LOG_WARN("failed to append header to curl list", K(ret), K(header_str));
          } else {
            header_list_ = new_header;
          }
        }
      }
      // Set the header list to curl
      if (OB_SUCC(ret) && header_list_ != nullptr) {
        if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, header_list_))) {
          ret = OB_CURL_ERROR;
          LOG_WARN("failed to set HTTP headers", K(ret), K(cc));
        }
      }
    }
    
    // write callback
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, ObCurlRestClient::write_callback))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set WRITEFUNCTION", K(ret), K(cc), K(concat_url));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &write_context))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set WRITEDATA", K(ret), K(cc), K(concat_url));
    }
  }
  return ret;
}

size_t ObCurlRestClient::write_callback(void *contents, size_t size, size_t nmemb, void *buffer)
{
  int ret = OB_SUCCESS;
  const bool chunk_size_overflow =
      0 != size && nmemb > static_cast<size_t>(INT64_MAX) / size;
  const size_t total_size = chunk_size_overflow ? 0 : size * nmemb;
  WriteContext *write_context = static_cast<WriteContext *>(buffer);
  if (OB_ISNULL(contents) || OB_ISNULL(buffer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for curl_write_callback", K(ret), K(contents), K(size), K(nmemb), K(buffer));
  } else if (OB_UNLIKELY(chunk_size_overflow)) {
    ret = OB_SIZE_OVERFLOW;
    write_context->size_overflow_ = true;
    LOG_WARN("curl response chunk size overflow", K(ret), K(size), K(nmemb));
  } else if (total_size > 0) {
    const int64_t current_size = write_context->body_.length();
    const int64_t max_response_body_size = write_context->max_response_body_size_;
    if (OB_UNLIKELY(current_size < 0
                    || total_size > static_cast<size_t>(INT64_MAX - current_size))
        || (max_response_body_size > 0
            && (current_size > max_response_body_size
                || total_size > static_cast<size_t>(max_response_body_size - current_size)))) {
      ret = OB_SIZE_OVERFLOW;
      write_context->size_overflow_ = true;
      LOG_WARN("curl response body exceeds size limit",
               K(ret),
               K(current_size),
               K(total_size),
               K(max_response_body_size));
    } else if (OB_FAIL(write_context->body_.append(static_cast<const char *>(contents),
                                                   static_cast<int64_t>(total_size)))) {
      LOG_WARN("failed to append to result",
               K(ret),
               K(write_context->body_),
               K(contents),
               K(total_size));
    }
  }
  return OB_SUCC(ret) ? total_size : 0;
}

}
}