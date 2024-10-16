/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef tutorial_TYPES_H
#define tutorial_TYPES_H

#include <iosfwd>

#include <thrift/Thrift.h>
#include <thrift/TApplicationException.h>
#include <thrift/TBase.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TTransport.h>

#include <functional>
#include <memory>


namespace tutorial {

typedef int32_t _File;

typedef int32_t _Flag;

/**
 * Data structures in thrift do not support unsigned int32, so we use i64 instead.
 * Remember to change the struct being used.
 */
typedef int64_t _Oid;

typedef int64_t _Off_t;

typedef std::string _Path;

typedef std::string _Page;

class _Stat_Resp;

class _XLog_Init_File_Resp;

class _Smgr_Relation;

typedef struct __Stat_Resp__isset {
  __Stat_Resp__isset() : _result(false), _stat_mode(false) {}
  bool _result :1;
  bool _stat_mode :1;
} __Stat_Resp__isset;

class _Stat_Resp : public virtual ::apache::thrift::TBase {
 public:

  _Stat_Resp(const _Stat_Resp&) noexcept;
  _Stat_Resp& operator=(const _Stat_Resp&) noexcept;
  _Stat_Resp() noexcept
             : _result(0),
               _stat_mode(0) {
  }

  virtual ~_Stat_Resp() noexcept;
  int32_t _result;
  int32_t _stat_mode;

  __Stat_Resp__isset __isset;

  void __set__result(const int32_t val);

  void __set__stat_mode(const int32_t val);

  bool operator == (const _Stat_Resp & rhs) const
  {
    if (!(_result == rhs._result))
      return false;
    if (!(_stat_mode == rhs._stat_mode))
      return false;
    return true;
  }
  bool operator != (const _Stat_Resp &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const _Stat_Resp & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(_Stat_Resp &a, _Stat_Resp &b);

std::ostream& operator<<(std::ostream& out, const _Stat_Resp& obj);

typedef struct __XLog_Init_File_Resp__isset {
  __XLog_Init_File_Resp__isset() : _fd(false), _use_existent(false) {}
  bool _fd :1;
  bool _use_existent :1;
} __XLog_Init_File_Resp__isset;

class _XLog_Init_File_Resp : public virtual ::apache::thrift::TBase {
 public:

  _XLog_Init_File_Resp(const _XLog_Init_File_Resp&) noexcept;
  _XLog_Init_File_Resp& operator=(const _XLog_Init_File_Resp&) noexcept;
  _XLog_Init_File_Resp() noexcept
                       : _fd(0),
                         _use_existent(0) {
  }

  virtual ~_XLog_Init_File_Resp() noexcept;
  int32_t _fd;
  int32_t _use_existent;

  __XLog_Init_File_Resp__isset __isset;

  void __set__fd(const int32_t val);

  void __set__use_existent(const int32_t val);

  bool operator == (const _XLog_Init_File_Resp & rhs) const
  {
    if (!(_fd == rhs._fd))
      return false;
    if (!(_use_existent == rhs._use_existent))
      return false;
    return true;
  }
  bool operator != (const _XLog_Init_File_Resp &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const _XLog_Init_File_Resp & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(_XLog_Init_File_Resp &a, _XLog_Init_File_Resp &b);

std::ostream& operator<<(std::ostream& out, const _XLog_Init_File_Resp& obj);

typedef struct __Smgr_Relation__isset {
  __Smgr_Relation__isset() : _spc_node(false), _db_node(false), _rel_node(false), _backend_id(false) {}
  bool _spc_node :1;
  bool _db_node :1;
  bool _rel_node :1;
  bool _backend_id :1;
} __Smgr_Relation__isset;

class _Smgr_Relation : public virtual ::apache::thrift::TBase {
 public:

  _Smgr_Relation(const _Smgr_Relation&) noexcept;
  _Smgr_Relation& operator=(const _Smgr_Relation&) noexcept;
  _Smgr_Relation() noexcept
                 : _spc_node(0),
                   _db_node(0),
                   _rel_node(0),
                   _backend_id(0) {
  }

  virtual ~_Smgr_Relation() noexcept;
  _Oid _spc_node;
  _Oid _db_node;
  _Oid _rel_node;
  int32_t _backend_id;

  __Smgr_Relation__isset __isset;

  void __set__spc_node(const _Oid val);

  void __set__db_node(const _Oid val);

  void __set__rel_node(const _Oid val);

  void __set__backend_id(const int32_t val);

  bool operator == (const _Smgr_Relation & rhs) const
  {
    if (!(_spc_node == rhs._spc_node))
      return false;
    if (!(_db_node == rhs._db_node))
      return false;
    if (!(_rel_node == rhs._rel_node))
      return false;
    if (!(_backend_id == rhs._backend_id))
      return false;
    return true;
  }
  bool operator != (const _Smgr_Relation &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const _Smgr_Relation & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(_Smgr_Relation &a, _Smgr_Relation &b);

std::ostream& operator<<(std::ostream& out, const _Smgr_Relation& obj);

} // namespace

#endif
