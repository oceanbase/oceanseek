/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/vector_index/ob_vector_kmeans_ctx.h"
#include "share/vector_type/ob_vector_cosine_distance.h"
#undef private
#undef protected

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include "lib/vector/ob_vector_util.h"

namespace oceanbase {
namespace common {
class TestVectorIndexKmeans : public ::testing::Test
{
public:
  TestVectorIndexKmeans()
  {}
  ~TestVectorIndexKmeans()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestVectorIndexKmeans);
};

enum DataSetType {
  FASHION_MNIST_784,
  GLOVE_25,
  TYPE_MAX,
};

struct KmeansDataSet
{
  KmeansDataSet()
    : is_inited_(false),
      topk_(0),
      type_(DataSetType::TYPE_MAX),
      train_count_(0),
      test_count_(0),
      dim_(0),
      dataset_distances_(),
      dataset_train_(),
      dataset_test_(),
      run_distances_(),
      allocator_(ObModIds::TEST)
  {}
  ~KmeansDataSet()
  { destroy(); }

  int init(DataSetType type);
  void destroy() { allocator_.reset(); }

  int set_topk(const int64_t topk, ObIAllocator &allocator);
  int clear_run(ObIAllocator &allocator);

  int load_dataset_distances();
  int load_dataset_train();
  int load_dataset_test();

  bool need_norm();
  
  using FuncPtrType = int (*)(const float* a, const float* b, const int64_t len, double& distance);
  static FuncPtrType distance_funcs[];

  static const int64_t neighbors = 100;
  static const int64_t DIM[TYPE_MAX];
  static const int64_t TRAIN_COUNT[TYPE_MAX];
  static const int64_t TEST_COUNT[TYPE_MAX];
  static const char* TRAIN_CSV_PATH[TYPE_MAX];
  static const char* TEST_CSV_PATH[TYPE_MAX];
  static const char* DISTANCES_CSV_PATH[TYPE_MAX];

  bool is_inited_;
  int64_t topk_;
  DataSetType type_;
  int64_t train_count_;
  int64_t test_count_;
  int64_t dim_;
  common::ObArrayWrap<double*> dataset_distances_;
  common::ObArrayWrap<float*> dataset_train_;
  common::ObArrayWrap<float*> dataset_test_;
  common::ObArrayWrap<double*> run_distances_;
  ObArenaAllocator allocator_;
};

KmeansDataSet::FuncPtrType KmeansDataSet::distance_funcs[TYPE_MAX] =
{
  ObVectorL2Distance<float>::l2_distance_func,
  ObVectorCosineDistance<float>::cosine_distance_func,
};

const int64_t KmeansDataSet::DIM[TYPE_MAX] =
{
  784,
  25,
};

const int64_t KmeansDataSet::TRAIN_COUNT[] = 
{
  60000L,
  1183514L,
};

const int64_t KmeansDataSet::TEST_COUNT[] = 
{
  10000L,
  10000L,
};

const char* KmeansDataSet::TRAIN_CSV_PATH[] = 
{
  "./data/fashion_mnist_train.csv",
  "./data/glove_25_train.csv",
};

const char* KmeansDataSet::TEST_CSV_PATH[] = 
{
  "./data/fashion_mnist_test.csv",
  "./data/glove_25_test.csv",
};

const char* KmeansDataSet::DISTANCES_CSV_PATH[] = 
{
  "./data/fashion_mnist_distances.csv",
  "./data/glove_25_distances.csv",
};

int KmeansDataSet::init(DataSetType type)
{
  int ret = OB_SUCCESS;
  if (TYPE_MAX <= type || type < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else {
    type_ = type;
    train_count_ = TRAIN_COUNT[type];
    test_count_ = TEST_COUNT[type];
    dim_ = DIM[type];
    
    if (OB_FAIL(dataset_distances_.allocate_array(allocator_, test_count_))) {
      LOG_WARN("failed to allocate array", K(ret));
    } else if (OB_FAIL(dataset_train_.allocate_array(allocator_, train_count_))) {
      LOG_WARN("failed to allocate array", K(ret));
    } else if (OB_FAIL(dataset_test_.allocate_array(allocator_, test_count_))) {
      LOG_WARN("failed to allocate array", K(ret));
    } else if (OB_FAIL(run_distances_.allocate_array(allocator_, test_count_))) {
      LOG_WARN("failed to allocate array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < test_count_; ++i) {
        double *distances = nullptr;
        float *test_vector = nullptr;
        if (OB_ISNULL(distances = static_cast<double*>(allocator_.alloc(neighbors * sizeof(double))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate distances", K(ret));
        } else if (FALSE_IT(dataset_distances_.at(i) = distances)) {
        } else if (OB_ISNULL(test_vector = static_cast<float*>(allocator_.alloc(dim_ * sizeof(float))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate test vector", K(ret));
        } else if (FALSE_IT(dataset_test_.at(i) = test_vector)) {
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < train_count_; ++i) {
        float *train_vector = nullptr;
        if (OB_ISNULL(train_vector = static_cast<float*>(allocator_.alloc(dim_ * sizeof(float))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate test vector", K(ret));
        } else if (FALSE_IT(dataset_train_.at(i) = train_vector)) {
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(load_dataset_train())) {
          LOG_WARN("failed to load_dataset_train");
        } else if (OB_FAIL(load_dataset_test())) {
          LOG_WARN("failed to load_dataset_test");
        } else if (OB_FAIL(load_dataset_distances())) {
          LOG_WARN("failed to load_dataset_distances");
        }
      }
    }
  }
  return ret;
}

int KmeansDataSet::set_topk(const int64_t topk, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (0 >= topk) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(topk));
  } else if (0 != topk_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old run exists", K(ret), K(topk_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < test_count_; ++i) {
      double *distances = nullptr;
      if (OB_ISNULL(distances = static_cast<double*>(allocator.alloc(topk * sizeof(double))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate distances", K(ret));
      } else {
        MEMSET(distances, 0, topk * sizeof(double));
        run_distances_.at(i) = distances;
      }
    }
    if (OB_SUCC(ret)) {
      topk_ = topk;
    }
  }
  return ret;
}

int KmeansDataSet::clear_run(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (0 != topk_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < test_count_; ++i) {
      double *distances = run_distances_.at(i);
      allocator.free(distances);
      run_distances_.at(i) = nullptr;
    }
    allocator.reset();
    topk_ = 0;
  }
  return ret;
}

int KmeansDataSet::load_dataset_distances()
{
  int ret = OB_SUCCESS;
  const char* file_name = DISTANCES_CSV_PATH[type_];
  std::ifstream file(file_name);
  std::string line;

  if (!file.is_open()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("could not open file", K(ret));
  } else {
    std::getline(file, line); // skip first row
    int64_t row_num = 0;
    while (std::getline(file, line) && row_num < test_count_) {
      std::stringstream lineStream(line);
      std::string cell;
      int64_t col_num = 0;
      while (std::getline(lineStream, cell, ',') && col_num < neighbors) {
        (dataset_distances_.at(row_num))[col_num] = std::stod(cell);
        ++col_num;
      }
      ++row_num;
    }
    file.close();
  }
  return ret;
}

int KmeansDataSet::load_dataset_train()
{
  int ret = OB_SUCCESS;
  const char* file_name = TRAIN_CSV_PATH[type_];
  std::ifstream file(file_name);
  std::string line;

  if (!file.is_open()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("could not open file", K(ret));
  } else {
    std::getline(file, line); // skip first row
    int64_t row_num = 0;
    while (std::getline(file, line) && row_num < train_count_) {
      std::stringstream lineStream(line);
      std::string cell;
      int64_t col_num = 0;
      while (std::getline(lineStream, cell, ',') && col_num < dim_) {
        (dataset_train_.at(row_num))[col_num] = std::stod(cell);
        ++col_num;
      }
      ++row_num;
    }
    file.close();
  }
  return ret;
}

int KmeansDataSet::load_dataset_test()
{
  int ret = OB_SUCCESS;
  const char* file_name = TEST_CSV_PATH[type_];
  std::ifstream file(file_name);
  std::string line;

  if (!file.is_open()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("could not open file", K(ret));
  } else {
    std::getline(file, line); // skip first row
    int64_t row_num = 0;
    while (std::getline(file, line) && row_num < test_count_) {
      std::stringstream lineStream(line);
      std::string cell;
      int64_t col_num = 0;
      while (std::getline(lineStream, cell, ',') && col_num < dim_) {
        (dataset_test_.at(row_num))[col_num] = std::stod(cell);
        ++col_num;
      }
      ++row_num;
    }
    file.close();
  }
  return ret;
}

bool KmeansDataSet::need_norm()
{
  return GLOVE_25 == type_;
}

class ObTestAnnSearchHelper : public share::ObVectorClusterHelper
{
public:
  ObTestAnnSearchHelper()
    : is_inited_(false),
      dataset_(nullptr),
      ctx_(nullptr),
      cluster_(),
      allocator_(ObModIds::TEST),
      tmp_allocator_(ObModIds::TEST)
  {}
  ~ObTestAnnSearchHelper()
  { destroy(); }

  int init(KmeansDataSet *dataset, ObSingleKmeansExecutor *ctx, const int64_t list, share::ObVectorNormalizeInfo *norm_info = nullptr);
  void destroy()
  { 
    allocator_.reset();
    tmp_allocator_.reset();
    is_inited_ = false;
  }

  // query
  int ann_search(const int64_t topk, const int64_t nprobe);

  // metrics
  int get_recall_value(const int64_t topk);

private:
  template<typename TCompare1, typename TCompare2>
  int inner_ann_search(const int64_t idx, float *vector, const int64_t dim, const int64_t topk, const int64_t nprobe);

private:
  bool is_inited_;
  KmeansDataSet *dataset_;
  ObSingleKmeansExecutor *ctx_;
  share::ObVectorNormalizeInfo *norm_info_;
  common::ObArrayWrap<ObSEArray<float*, 64>> cluster_;
  ObArenaAllocator allocator_;
  ObArenaAllocator tmp_allocator_;
};

int ObTestAnnSearchHelper::init(KmeansDataSet *dataset, ObSingleKmeansExecutor *ctx, const int64_t list, share::ObVectorNormalizeInfo *norm_info)
{
  int ret = OB_SUCCESS;
  ObKmeansAlgo *algo = nullptr;
  if (OB_ISNULL(dataset) || OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ctx->get_kmeans_algo(algo))) {
    LOG_WARN("fail to get kmeans algo", K(ret));
  } else {
    // get centers
    share::ObCentersBuffer<float> &centers = algo->get_cur_centers();
    if (list != centers.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("list is not equal to center count", K(ret), K(list), K(centers.count()));
    } else if (OB_FAIL(cluster_.allocate_array(allocator_, list))) {
      LOG_WARN("failed to alloacte array", K(ret));
    } else {
      int64_t center_idx = 0;
      const int64_t dim = dataset->dim_;
      for (int64_t i = 0; OB_SUCC(ret) && i < dataset->dataset_train_.count(); ++i) {
        if (OB_FAIL(centers.get_nearest_center(dim, dataset->dataset_train_.at(i), center_idx))) {
          LOG_WARN("failed to get nearest center", K(ret));
        } else if (OB_FAIL(cluster_.at(center_idx).push_back(dataset->dataset_train_.at(i)))) {
          LOG_WARN("failed to push back array", K(ret));
        }
      }
      if (OB_SUCC(ret)){
        dataset_ = dataset;
        ctx_ = ctx;
        norm_info_ = norm_info;
        is_inited_ = true;
        LOG_INFO("finish divide the data into clusters", K(ret));
      }
    }
  }
  return ret;
}

template<typename TCompare1, typename TCompare2>
int ObTestAnnSearchHelper::inner_ann_search(const int64_t idx, float *vector, const int64_t dim, const int64_t topk, const int64_t nprobe)
{
  int ret = OB_SUCCESS;
  TCompare1 compare1;
  TCompare2 compare2;
  common::ObBinaryHeap<HeapCenterItem, TCompare1, 64> center_heap(compare1);
  common::ObBinaryHeap<HeapItem, TCompare2, 64> result_heap(compare2);
  double distance = DBL_MAX;

  // normalize if needed
  float *norm_vector = nullptr;
  if (OB_NOT_NULL(norm_info_)) {
    if (OB_ISNULL(norm_vector = static_cast<float*>(tmp_allocator_.alloc(dim * sizeof(float))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc norm vector", K(ret));
    } else if (FALSE_IT(MEMSET(norm_vector, 0, dim * sizeof(float)))) {
    } else if (OB_FAIL(norm_info_->normalize_func_(dim, vector, norm_vector, nullptr))) {
      LOG_WARN("failed to normalize vector", K(ret));
    }
  }
  
  // get the nearest nprobe centers
  float *data = norm_vector == nullptr ? vector : norm_vector;
  // get centers
  ObKmeansAlgo *algo = nullptr;
  if (FAILEDx(ctx_->get_kmeans_algo(algo))) {
    LOG_WARN("fail to get kmeans algo", K(ret));
  } else {
    share::ObCentersBuffer<float> &centers = algo->get_cur_centers();
    for (int64_t i = 0; OB_SUCC(ret) && i < centers.count(); ++i) {
      if (OB_FAIL(ObVectorL2Distance<float>::l2_distance_func(data, centers.at(i), dim, distance))) {
        LOG_WARN("failed to calc l2 distance", K(ret));
      } else if (center_heap.count() < nprobe) {
        if (OB_FAIL(center_heap.push(HeapCenterItem(distance, i)))) {
          LOG_WARN("failed to push center heap", K(ret), K(i), K(distance));
        }
      } else {
        const HeapCenterItem &top = center_heap.top();
        HeapCenterItem tmp(distance, i);
        if (compare1(tmp, top)) {
          HeapCenterItem tmp(distance, i);
          if (OB_FAIL(center_heap.replace_top(tmp))) {
            LOG_WARN("failed to replace top", K(ret), K(tmp));
          }
        }
      }
    }
  }
  // get the topk
  for (int64_t i = 0; OB_SUCC(ret) && i < center_heap.count(); ++i) {
    const int64_t center_idx = center_heap.at(i).center_idx_;
    for (int64_t j = 0; OB_SUCC(ret) && j < cluster_.at(center_idx).count(); ++j) {
      float *data = cluster_.at(center_idx).at(j);
      if (OB_FAIL(KmeansDataSet::distance_funcs[dataset_->type_](vector, data, dim, distance))) {
        LOG_WARN("failed to calc distance", K(ret));
      } else if (result_heap.count() < topk) {
        if (OB_FAIL(result_heap.push(HeapItem(distance)))) {
          LOG_WARN("failed to push center heap", K(ret), K(i), K(distance));
        }
      } else {
        const HeapItem &top = result_heap.top();
        HeapItem tmp(distance);
        if (compare2(tmp, top)) {
          if (OB_FAIL(result_heap.replace_top(tmp))) {
            LOG_WARN("failed to replace top", K(ret), K(tmp));
          }
        }
      }
    }
  }
  // update run distances
  for (int64_t i = 0; OB_SUCC(ret) && i < result_heap.count(); ++i) {
    dataset_->run_distances_.at(idx)[i] = result_heap.at(i).distance_;
  }
  return ret;
}

int ObTestAnnSearchHelper::ann_search(const int64_t topk, const int64_t nprobe)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTestAnnSearchHelper is not inited", K(ret));
  } else {
    LOG_INFO("start ann search", K(topk), K(nprobe));
    dataset_->clear_run(tmp_allocator_);
    if (OB_FAIL(dataset_->set_topk(topk, tmp_allocator_))) {
      LOG_WARN("failed to set topk", K(ret));
    }
    const int64_t total_count = dataset_->dataset_test_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < total_count; ++i) {
      float *vector = dataset_->dataset_test_.at(i);
      if (OB_SUCCESS != (ret = inner_ann_search<MaxHeapCompare, MaxHeapCompare>(i, vector, dataset_->dim_, topk, nprobe))) {
        LOG_WARN("failed to inner ann search", K(ret));
      } else if (i != 0 && i % 1000 == 0) {
        LOG_INFO("ann search", K(total_count), K(i));
      }
    }
  }
  return ret;
}

int ObTestAnnSearchHelper::get_recall_value(const int64_t topk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTestAnnSearchHelper is not inited", K(ret));
  } else {
    int64_t total_recall = 0;
    double epsilon = 1e-3;
    for (int64_t i = 0; i < dataset_->run_distances_.count(); ++i) {
      for (int64_t j = 0; j < topk; ++j) {
        if (dataset_->run_distances_.at(i)[j] <= (dataset_->dataset_distances_.at(i)[topk] + epsilon)) {
          ++total_recall;
        }
      }
    }
    double recall = (double)total_recall / dataset_->run_distances_.count();
    recall = recall / topk;
    LOG_INFO("metrics", K(topk), K(recall));
  }
  return ret;
}

TEST_F(TestVectorIndexKmeans, hgraph_centroid_distance_preserves_existing_metrics)
{
  // L1 chooses center 10 (distance 3), while squared L2 chooses center 20 (distance 8).
  // The non-unit query would produce a negative distance if IP leaked into training.
  float centers[] = {5.0f, 0.0f, 4.0f, 2.0f};
  int64_t ids[] = {10, 20};
  float query[] = {2.0f, 0.0f};
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L1, share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  const int64_t expected_ids[] = {10, 20, 20, 20};
  const float expected_distances[] = {3.0f, 8.0f, 8.0f, 8.0f};

  struct HgraphAllocator : public vsag::Allocator
  {
    std::string Name() override { return "KmeansHgraphTest"; }
    void *Allocate(size_t size) override { return std::malloc(size); }
    void Deallocate(void *ptr) override { std::free(ptr); }
    void *Reallocate(void *ptr, size_t size) override { return std::realloc(ptr, size); }
  };
  struct HgraphGuard
  {
    ~HgraphGuard()
    {
      if (nullptr != result_ids_) {
        allocator_.Deallocate(const_cast<int64_t *>(result_ids_));
      }
      if (nullptr != distances_) {
        allocator_.Deallocate(const_cast<float *>(distances_));
      }
      if (nullptr != index_) {
        obvectorutil::delete_index(index_);
      }
    }
    HgraphAllocator allocator_;
    obvsag::VectorIndexPtr index_ = nullptr;
    const int64_t *result_ids_ = nullptr;
    const float *distances_ = nullptr;
  };

  ASSERT_TRUE(obvsag::is_init());
  for (int64_t i = 0; i < 4; ++i) {
    SCOPED_TRACE(static_cast<int>(metrics[i]));
    HgraphGuard graph;
    const auto metric = share::ObElkanKmeansAlgo::get_hgraph_distance_algorithm(metrics[i]);
    ASSERT_EQ(OB_SUCCESS, obvectorutil::create_index(
        graph.index_, share::VIAT_HGRAPH, "float32", share::VEC_INDEX_ALGTH[metric],
        2, 16, 200, 64, &graph.allocator_));
    ASSERT_EQ(OB_SUCCESS, obvectorutil::build_index(graph.index_, centers, ids, 2, 2));
    const char *extra_info = nullptr;
    int64_t result_size = 0;
    ASSERT_EQ(OB_SUCCESS, obvectorutil::knn_search(
        graph.index_, query, 2, 1, graph.distances_, graph.result_ids_, extra_info,
        result_size, 64, nullptr, false, false, 1.0f, &graph.allocator_, false));
    ASSERT_EQ(1, result_size);
    ASSERT_NE(nullptr, graph.result_ids_);
    ASSERT_NE(nullptr, graph.distances_);
    EXPECT_EQ(expected_ids[i], graph.result_ids_[0]);
    EXPECT_FLOAT_EQ(expected_distances[i], graph.distances_[0]);
    EXPECT_GE(graph.distances_[0], 0.0f);
  }
}

TEST_F(TestVectorIndexKmeans, l1_distance_dispatch)
{
  const float lhs[] = {0.0f, 0.0f};
  const float rhs[] = {3.0f, 4.0f};
  float distance = 0.0f;

  ASSERT_EQ(OB_SUCCESS,
      ObKmeansAlgo::calc_kmeans_distance(lhs, rhs, 2, share::VIDA_L1, distance));
  EXPECT_FLOAT_EQ(7.0f, distance);

  ASSERT_EQ(OB_SUCCESS,
      ObKmeansAlgo::calc_kmeans_distance(lhs, rhs, 2, share::VIDA_L2, distance));
  EXPECT_FLOAT_EQ(25.0f, distance);
}

TEST_F(TestVectorIndexKmeans, l1_and_l2_choose_different_centers)
{
  float query[] = {0.0f, 0.0f};
  float centers[] = {
      3.0f, 0.0f,
      2.0f, 2.0f,
  };
  ObArenaAllocator allocator(ObModIds::TEST);
  ObVectorClusterHelper l1_helper;
  ObVectorClusterHelper l2_helper;
  int64_t center_idx = 0;

  ASSERT_EQ(OB_SUCCESS,
      l1_helper.get_nearest_probe_centers(
          query, 2, centers, 2, 2, 1, allocator, nullptr, 0, -1,
          sql::ObExprVectorDistance::ObVecDisType::MANHATTAN));
  ASSERT_EQ(OB_SUCCESS, l1_helper.get_center_idx(0, center_idx));
  EXPECT_EQ(1, center_idx);

  ASSERT_EQ(OB_SUCCESS,
      l2_helper.get_nearest_probe_centers(
          query, 2, centers, 2, 2, 1, allocator, nullptr, 0, -1,
          sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED));
  ASSERT_EQ(OB_SUCCESS, l2_helper.get_center_idx(0, center_idx));
  EXPECT_EQ(2, center_idx);
}

// TEST_F(TestVectorIndexKmeans, DISABLED_fashion_mnist)
// {
//   KmeansDataSet dataset;
//   ASSERT_EQ(OB_SUCCESS, dataset.init(FASHION_MNIST_784));
//   // lists 60
//   ObSingleKmeansExecutor kmeans_ctx;
//   ASSERT_EQ(OB_SUCCESS, kmeans_ctx.init(
//     ObKmeansAlgoType::KAT_ELKAN,
//     OB_SERVER_TENANT_ID/*tenant_id*/,
//     60/*lists*/,
//     200/*sample_per_nlist*/,
//     784/*dim*/,
//     share::VIDA_L2/*unused*/));
//   // do sample
//   for (int64_t i = 0; i < dataset.dataset_train_.count(); ++i) {
//     ASSERT_EQ(OB_SUCCESS, kmeans_ctx.append_sample_vector(dataset.dataset_train_.at(i)));
//   }
//   // build
//   ASSERT_EQ(OB_SUCCESS, kmeans_ctx.build());
  
//   ObTestAnnSearchHelper helper;
//   ASSERT_EQ(OB_SUCCESS, helper.init(&dataset, &kmeans_ctx, 60/*lists*/));
//   // test top 10
//   // nprobe 1
//   ASSERT_EQ(OB_SUCCESS, helper.ann_search(10/*topk*/, 1/*nprobe*/));
//   // metrics
//   ASSERT_EQ(OB_SUCCESS, helper.get_recall_value(10/*topk*/));
//   // nprobe 5
//   ASSERT_EQ(OB_SUCCESS, helper.ann_search(10/*topk*/, 5/*nprobe*/));
//   // metrics
//   ASSERT_EQ(OB_SUCCESS, helper.get_recall_value(10/*topk*/));
// }

// TEST_F(TestVectorIndexKmeans, DISABLED_glove_25)
// {
//   KmeansDataSet dataset;
//   ASSERT_EQ(OB_SUCCESS, dataset.init(GLOVE_25));
//   share::ObVectorNormalizeInfo norm_info;
//   // lists 1000
//   ObSingleKmeansExecutor kmeans_ctx;
//   ASSERT_EQ(OB_SUCCESS, kmeans_ctx.init(
//     ObKmeansAlgoType::KAT_ELKAN,
//     OB_SERVER_TENANT_ID/*tenant_id*/,
//     1000/*lists*/,
//     50/*sample_per_nlist*/,
//     25/*dim*/,
//     share::VIDA_L2/*unused*/,
//     &norm_info));
//   // do sample
//   for (int64_t i = 0; i < dataset.dataset_train_.count(); ++i) {
//     ASSERT_EQ(OB_SUCCESS, kmeans_ctx.append_sample_vector(dataset.dataset_train_.at(i)));
//   }
//   // build
//   ASSERT_EQ(OB_SUCCESS, kmeans_ctx.build());
  
//   ObTestAnnSearchHelper helper;
//   ASSERT_EQ(OB_SUCCESS, helper.init(&dataset, &kmeans_ctx, 1000/*lists*/, &norm_info));
//   // test top 10
//   // nprobe 1
//   ASSERT_EQ(OB_SUCCESS, helper.ann_search(10/*topk*/, 1/*nprobe*/));
//   // metrics
//   ASSERT_EQ(OB_SUCCESS, helper.get_recall_value(10/*topk*/));
//   // nprobe 5
//   ASSERT_EQ(OB_SUCCESS, helper.ann_search(10/*topk*/, 5/*nprobe*/));
//   // metrics
//   ASSERT_EQ(OB_SUCCESS, helper.get_recall_value(10/*topk*/));
//   // nprobe 10
//   ASSERT_EQ(OB_SUCCESS, helper.ann_search(10/*topk*/, 10/*nprobe*/));
//   // metrics
//   ASSERT_EQ(OB_SUCCESS, helper.get_recall_value(10/*topk*/));
// }

} // namespace common
} // namespace oceanbase

// use ./test_kmeans --gtest_also_run_disabled_tests to test
int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
