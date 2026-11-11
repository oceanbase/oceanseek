/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifdef NEED_REGISTER_ATOMIC_PROTOCOL

/*
 * REGISTER_ATOMIC_PROTOCOL: register atomic protocol
 * *********************************************************************
 * Params:
 *  TYPE_NAME:    enum name, e.g. MINI_SSTABLE / TABLET_META / LS_META.
 *                Used as ObAtomicProtocolTypeEnum::<TYPE_NAME>.
 *  ID:           protocol-type unique numeric id.
 *  TYPE_LEVEL:   protocol-semantic level. The token is concatenated as
 *                ObAtomicProtocolLevel::<TYPE_LEVEL>, so it MUST be one of:
 *                  LS      -- LS-scoped metadata, dispatched to the LS
 *                             atomic-protocol class
 *                             (e.g. LS_META, CHECKPOINT)
 *                  TABLET  -- tablet-scoped metadata that is *not* an
 *                             sstable, dispatched to ObTabletAtomicProtocol
 *                             (e.g. TABLET_META)
 *                  SSTABLE -- tablet-scoped sstable metadata, dispatched
 *                             to ObSSTableAtomicProtocol
 *                             (MINI / MINOR / MAJOR)
 *  SSWRITER_TYPE: sswriter type token, used as ObSSWriterType::<SSWRITER_TYPE>.
 *  CLASS_TYPE:   USER_DEFINE / DEFAULT
 *                  DEFAULT     -- use the default ObAtomicDefaultFile
 *                  USER_DEFINE -- use a user-defined class
 * ********************************************************************
 */

  REGISTER_ATOMIC_PROTOCOL(MINI_SSTABLE,                                       /* type name */ \
                        1,                                                     /* type id */ \
                        SSTABLE,                                                /* type level */ \
                        FREEZE,                                                /* sswriter type */ \
                        USER_DEFINE                                            /* class type*/ \
                        )
  REGISTER_ATOMIC_PROTOCOL(MINOR_SSTABLE,                                      /* type name */ \
                        2,                                                     /* type id */ \
                        SSTABLE,                                                /* type level */ \
                        COMPACTION,                                            /* sswriter type */ \
                        USER_DEFINE                                            /* class type*/ \
                        )
  REGISTER_ATOMIC_PROTOCOL(LS_META,                                            /* type name */ \
                        3,                                                     /* type id */ \
                        LS,                                                    /* type level */ \
                        META,                                                  /* sswriter type */ \
                        DEFAULT                                                /* class type*/ \
                        )
  REGISTER_ATOMIC_PROTOCOL(TABLET_META,                                        /* type name */ \
                        4,                                                     /* type id */ \
                        TABLET,                                                /* type level */ \
                        META,                                                  /* sswriter type */ \
                        USER_DEFINE                                            /* class type*/ \
                        )
  REGISTER_ATOMIC_PROTOCOL(MAJOR_SSTABLE,                                      /* type name */ \
                      9,                                                       /* type id */ \
                      SSTABLE,                                                  /* type level */ \
                      COMPACTION,                                              /* sswriter type */ \
                      USER_DEFINE                                              /* class type*/ \
                      )
  REGISTER_ATOMIC_PROTOCOL(CHECKPOINT,                                         /* type name */ \
                      10,                                                      /* type id */ \
                      LS,                                                      /* type level */ \
                      META,                                                    /* sswriter type */ \
                      USER_DEFINE                                              /* class type*/ \
                      )
  REGISTER_ATOMIC_PROTOCOL(INC_MAJOR_COMPACTION_SSTABLE,                        /* 类型名 */ \
                      11,                                                        /* 类型 ID */ \
                      SSTABLE,                                                   /* 类型层级 */ \
                      COMPACTION,                                                /* sswriter 类型 */ \
                      USER_DEFINE                                                /* 类类型 */ \
                      )
  REGISTER_ATOMIC_PROTOCOL(NON_IDEM_MAJOR_SSTABLE,                       /* 类型名 */ \
                      12,                                                        /* 类型 ID */ \
                      SSTABLE,                                                   /* 类型层级 */ \
                      COMPACTION,                                                /* sswriter 类型 */ \
                      USER_DEFINE                                                /* 类类型 */ \
                      )
#endif
