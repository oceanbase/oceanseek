/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DTL_PROCESSOR_H
#define OB_DTL_PROCESSOR_H

#include "share/interrupt/ob_global_interrupt_call.h"
#include "lib/oblog/ob_log.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/dtl/ob_dtl_msg.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "common/row/ob_row_iterator.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase {
namespace sql {
namespace dtl {

class ObDtlInterruptProc
{
public:
  virtual int process(const common::ObInterruptCode &ic) = 0;
};

class ObDtlMsgIterator
{
public:
  virtual bool has_next() = 0;
  virtual void set_iterator_end() = 0;
  virtual int get_next_row(common::ObNewRow &row) = 0;
  virtual int get_next_row(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx) = 0;
};

class ObDtlPacketProcBase
{
public:
  enum class ProcessStage
  {
    INIT,
    DECODE,
    CALLBACK,
    FINISH
  };

  ObDtlPacketProcBase() : process_stage_(ProcessStage::INIT) {}
  virtual ~ObDtlPacketProcBase() = default;
  virtual ObDtlMsgType get_proc_type() const = 0;
  virtual int process(const ObDtlLinkedBuffer &buffer, bool &transferred) = 0;
  // 阶段记录消费进度，失败时由 channel 决定是否回退完整消息。
  ProcessStage get_process_stage() const { return process_stage_; }
  void set_process_stage(ProcessStage stage) { process_stage_ = stage; }
private:
  ProcessStage process_stage_;
};

template <class Packet>
class ObDtlPacketProc
    : public ObDtlPacketProcBase
{
public:
  ObDtlMsgType get_proc_type() const override { return Packet::type(); }
  int process(const ObDtlLinkedBuffer &buffer, bool &transferred) override;
private:
  int decode(const ObDtlLinkedBuffer &buffer);
  virtual int process(const Packet &pkt) = 0;
private:
  Packet pkt_;
};

template <class Packet>
int ObDtlPacketProc<Packet>::decode(const ObDtlLinkedBuffer &buffer)
{
  using common::OB_SUCCESS;
  int ret = OB_SUCCESS;
  const char *buf = buffer.buf();
  int64_t size = buffer.size();
  int64_t &pos = buffer.pos();
  if (OB_FAIL(common::serialization::decode(buf, size, pos, pkt_))) {
    SQL_DTL_LOG(WARN, "decode DTL packet fail", K(size), K(pos));
  } else {
  }
  return ret;
}

template <class Packet>
int ObDtlPacketProc<Packet>::process(const ObDtlLinkedBuffer &buffer, bool &transferred)
{
  int ret = common::OB_SUCCESS;
  transferred = false;
  if (buffer.pos() == buffer.size()) {
      // last row
    ret = OB_ITER_END;
    set_process_stage(ProcessStage::FINISH);
  } else if (OB_SUCC(decode(buffer))) {
    // callback 可能产生副作用，开始执行后即使失败也不能重放。
    set_process_stage(ProcessStage::CALLBACK);
    ret = process(pkt_);
    if (OB_SUCC(ret)) {
      set_process_stage(ProcessStage::FINISH);
    }
  }
  // packet 处理完成后需要释放内存
  // 否则会有内存泄漏
  // 
  // 无论处理是否成功，都需要reset对应的packet
  pkt_.reset();
  return ret;
}

template <class Packet>
class ObDtlPacketEmptyProc
    : public ObDtlPacketProcBase
{
public:
  ObDtlPacketEmptyProc() = default;
  virtual ~ObDtlPacketEmptyProc() = default;
  ObDtlMsgType get_proc_type() const override { return Packet::type(); }
  int process(const ObDtlLinkedBuffer &buffer, bool &transferred) override;
private:
  int decode(const ObDtlLinkedBuffer &buffer);
};

template <class Packet>
int ObDtlPacketEmptyProc<Packet>::decode(const ObDtlLinkedBuffer &buffer)
{
  using common::OB_SUCCESS;
  int ret = OB_SUCCESS;
  const char *buf = buffer.buf();
  int64_t size = buffer.size();
  int64_t &pos = buffer.pos();
  Packet pkt;
  if (OB_FAIL(common::serialization::decode(buf, size, pos, pkt))) {
    SQL_DTL_LOG(WARN, "decode DTL packet fail", K(size), K(pos));
  }
  return ret;
}

template <class Packet>
int ObDtlPacketEmptyProc<Packet>::process(const ObDtlLinkedBuffer &buffer, bool &transferred)
{
  int ret = common::OB_SUCCESS;
  transferred = false;
  if (buffer.pos() == buffer.size()) {
      // last row
    ret = OB_ITER_END;
    set_process_stage(ProcessStage::FINISH);
  } else if (OB_FAIL(decode(buffer))) {
    // do nothing after decode. as we intend to discard the packet
  } else {
    set_process_stage(ProcessStage::FINISH);
  }
  return ret;
}

}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_PROCESSOR_H */
