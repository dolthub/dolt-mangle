// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package serial

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Timestamp struct {
	_tab flatbuffers.Struct
}

func (rcv *Timestamp) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Timestamp) Table() flatbuffers.Table {
	return rcv._tab.Table
}

func (rcv *Timestamp) Time() uint64 {
	return rcv._tab.GetUint64(rcv._tab.Pos + flatbuffers.UOffsetT(0))
}
func (rcv *Timestamp) MutateTime(n uint64) bool {
	return rcv._tab.MutateUint64(rcv._tab.Pos+flatbuffers.UOffsetT(0), n)
}

func CreateTimestamp(builder *flatbuffers.Builder, time uint64) flatbuffers.UOffsetT {
	builder.Prep(8, 8)
	builder.PrependUint64(time)
	return builder.Offset()
}

type Ref struct {
	_tab flatbuffers.Table
}

func GetRootAsRef(buf []byte, offset flatbuffers.UOffsetT) *Ref {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Ref{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsRef(buf []byte, offset flatbuffers.UOffsetT) *Ref {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Ref{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *Ref) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Ref) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Ref) Hash(j int) int8 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetInt8(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *Ref) HashLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *Ref) MutateHash(j int, n int8) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateInt8(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func RefStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func RefAddHash(builder *flatbuffers.Builder, hash flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(hash), 0)
}
func RefStartHashVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func RefEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type StoreRoot struct {
	_tab flatbuffers.Table
}

func GetRootAsStoreRoot(buf []byte, offset flatbuffers.UOffsetT) *StoreRoot {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &StoreRoot{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsStoreRoot(buf []byte, offset flatbuffers.UOffsetT) *StoreRoot {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &StoreRoot{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *StoreRoot) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *StoreRoot) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *StoreRoot) Refs(obj *RefMap) *RefMap {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(RefMap)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *StoreRoot) Time(obj *Timestamp) *Timestamp {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := o + rcv._tab.Pos
		if obj == nil {
			obj = new(Timestamp)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func StoreRootStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func StoreRootAddRefs(builder *flatbuffers.Builder, refs flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(refs), 0)
}
func StoreRootAddTime(builder *flatbuffers.Builder, time flatbuffers.UOffsetT) {
	builder.PrependStructSlot(1, flatbuffers.UOffsetT(time), 0)
}
func StoreRootEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type DatabaseRoot struct {
	_tab flatbuffers.Table
}

func GetRootAsDatabaseRoot(buf []byte, offset flatbuffers.UOffsetT) *DatabaseRoot {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &DatabaseRoot{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsDatabaseRoot(buf []byte, offset flatbuffers.UOffsetT) *DatabaseRoot {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &DatabaseRoot{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *DatabaseRoot) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *DatabaseRoot) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *DatabaseRoot) Tables(obj *RefMap) *RefMap {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(RefMap)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *DatabaseRoot) ForeignKeys(obj *ForeignKey, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *DatabaseRoot) ForeignKeysLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func DatabaseRootStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func DatabaseRootAddTables(builder *flatbuffers.Builder, tables flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(tables), 0)
}
func DatabaseRootAddForeignKeys(builder *flatbuffers.Builder, foreignKeys flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(foreignKeys), 0)
}
func DatabaseRootStartForeignKeysVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func DatabaseRootEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type Table struct {
	_tab flatbuffers.Table
}

func GetRootAsTable(buf []byte, offset flatbuffers.UOffsetT) *Table {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Table{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsTable(buf []byte, offset flatbuffers.UOffsetT) *Table {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Table{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *Table) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Table) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Table) Name() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Table) Schema(obj *Ref) *Ref {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Ref)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *Table) PrimaryIndex(obj *Ref) *Ref {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Ref)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *Table) SecondaryIndexes(obj *RefMap) *RefMap {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(RefMap)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func TableStart(builder *flatbuffers.Builder) {
	builder.StartObject(4)
}
func TableAddName(builder *flatbuffers.Builder, name flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(name), 0)
}
func TableAddSchema(builder *flatbuffers.Builder, schema flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(schema), 0)
}
func TableAddPrimaryIndex(builder *flatbuffers.Builder, primaryIndex flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(primaryIndex), 0)
}
func TableAddSecondaryIndexes(builder *flatbuffers.Builder, secondaryIndexes flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(secondaryIndexes), 0)
}
func TableEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type Commit struct {
	_tab flatbuffers.Table
}

func GetRootAsCommit(buf []byte, offset flatbuffers.UOffsetT) *Commit {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Commit{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsCommit(buf []byte, offset flatbuffers.UOffsetT) *Commit {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Commit{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *Commit) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Commit) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Commit) Root(obj *Ref) *Ref {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Ref)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *Commit) ParentList(obj *Ref, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Commit) ParentListLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *Commit) ParentClosure(obj *Ref) *Ref {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Ref)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *Commit) Meta(obj *CommitMeta) *CommitMeta {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(CommitMeta)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func CommitStart(builder *flatbuffers.Builder) {
	builder.StartObject(4)
}
func CommitAddRoot(builder *flatbuffers.Builder, root flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(root), 0)
}
func CommitAddParentList(builder *flatbuffers.Builder, parentList flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(parentList), 0)
}
func CommitStartParentListVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func CommitAddParentClosure(builder *flatbuffers.Builder, parentClosure flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(parentClosure), 0)
}
func CommitAddMeta(builder *flatbuffers.Builder, meta flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(meta), 0)
}
func CommitEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type CommitMeta struct {
	_tab flatbuffers.Table
}

func GetRootAsCommitMeta(buf []byte, offset flatbuffers.UOffsetT) *CommitMeta {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &CommitMeta{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsCommitMeta(buf []byte, offset flatbuffers.UOffsetT) *CommitMeta {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &CommitMeta{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *CommitMeta) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *CommitMeta) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *CommitMeta) Name() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *CommitMeta) Email() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *CommitMeta) Desc() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *CommitMeta) Timestamp(obj *Timestamp) *Timestamp {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		x := o + rcv._tab.Pos
		if obj == nil {
			obj = new(Timestamp)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *CommitMeta) UserTimestamp(obj *Timestamp) *Timestamp {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		x := o + rcv._tab.Pos
		if obj == nil {
			obj = new(Timestamp)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *CommitMeta) Metaversion() uint16 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return rcv._tab.GetUint16(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *CommitMeta) MutateMetaversion(n uint16) bool {
	return rcv._tab.MutateUint16Slot(14, n)
}

func CommitMetaStart(builder *flatbuffers.Builder) {
	builder.StartObject(6)
}
func CommitMetaAddName(builder *flatbuffers.Builder, name flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(name), 0)
}
func CommitMetaAddEmail(builder *flatbuffers.Builder, email flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(email), 0)
}
func CommitMetaAddDesc(builder *flatbuffers.Builder, desc flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(desc), 0)
}
func CommitMetaAddTimestamp(builder *flatbuffers.Builder, timestamp flatbuffers.UOffsetT) {
	builder.PrependStructSlot(3, flatbuffers.UOffsetT(timestamp), 0)
}
func CommitMetaAddUserTimestamp(builder *flatbuffers.Builder, userTimestamp flatbuffers.UOffsetT) {
	builder.PrependStructSlot(4, flatbuffers.UOffsetT(userTimestamp), 0)
}
func CommitMetaAddMetaversion(builder *flatbuffers.Builder, metaversion uint16) {
	builder.PrependUint16Slot(5, metaversion, 0)
}
func CommitMetaEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type Tag struct {
	_tab flatbuffers.Table
}

func GetRootAsTag(buf []byte, offset flatbuffers.UOffsetT) *Tag {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Tag{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsTag(buf []byte, offset flatbuffers.UOffsetT) *Tag {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Tag{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *Tag) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Tag) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Tag) Commit(obj *Ref) *Ref {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Ref)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *Tag) Meta(obj *TagMeta) *TagMeta {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(TagMeta)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func TagStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func TagAddCommit(builder *flatbuffers.Builder, commit flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(commit), 0)
}
func TagAddMeta(builder *flatbuffers.Builder, meta flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(meta), 0)
}
func TagEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

type TagMeta struct {
	_tab flatbuffers.Table
}

func GetRootAsTagMeta(buf []byte, offset flatbuffers.UOffsetT) *TagMeta {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &TagMeta{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsTagMeta(buf []byte, offset flatbuffers.UOffsetT) *TagMeta {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &TagMeta{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *TagMeta) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *TagMeta) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *TagMeta) Name() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TagMeta) Email() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TagMeta) Desc() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TagMeta) Timestamp(obj *Timestamp) *Timestamp {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		x := o + rcv._tab.Pos
		if obj == nil {
			obj = new(Timestamp)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *TagMeta) UserTimestamp(obj *Timestamp) *Timestamp {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		x := o + rcv._tab.Pos
		if obj == nil {
			obj = new(Timestamp)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *TagMeta) Metaversion() uint16 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return rcv._tab.GetUint16(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *TagMeta) MutateMetaversion(n uint16) bool {
	return rcv._tab.MutateUint16Slot(14, n)
}

func TagMetaStart(builder *flatbuffers.Builder) {
	builder.StartObject(6)
}
func TagMetaAddName(builder *flatbuffers.Builder, name flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(name), 0)
}
func TagMetaAddEmail(builder *flatbuffers.Builder, email flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(email), 0)
}
func TagMetaAddDesc(builder *flatbuffers.Builder, desc flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(desc), 0)
}
func TagMetaAddTimestamp(builder *flatbuffers.Builder, timestamp flatbuffers.UOffsetT) {
	builder.PrependStructSlot(3, flatbuffers.UOffsetT(timestamp), 0)
}
func TagMetaAddUserTimestamp(builder *flatbuffers.Builder, userTimestamp flatbuffers.UOffsetT) {
	builder.PrependStructSlot(4, flatbuffers.UOffsetT(userTimestamp), 0)
}
func TagMetaAddMetaversion(builder *flatbuffers.Builder, metaversion uint16) {
	builder.PrependUint16Slot(5, metaversion, 0)
}
func TagMetaEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
