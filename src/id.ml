module type FIELD = sig
  type t

  val byte_size : int
  val read : Cstruct.t -> int -> t
  val write : Cstruct.t -> int -> t -> unit
end

module type S = sig
  type t [@@deriving repr]

  include FIELD with type t := t

  val to_string : t -> string
  val of_int : int -> t
  val of_int64 : Int64.t -> t
  val add : t -> int -> t
  val succ : t -> t
  val equal : t -> t -> bool
  val compare : t -> t -> int
  val to_int64 : t -> Int64.t
end

module I8 : S = struct
  type t = int

  let t =
    Repr.map
      (Repr.string_of (`Fixed 1))
      (fun s -> Char.code s.[0])
      (fun i -> String.make 1 (Char.chr i))

  let byte_size = 1
  let read cstruct offset = Cstruct.get_uint8 cstruct offset
  let write cstruct offset v = Cstruct.set_uint8 cstruct offset v
  let to_string = string_of_int
  let of_int i = i
  let of_int64 = Int64.to_int
  let add t x = t + x
  let succ x = x + 1
  let equal = Int.equal
  let compare = Int.compare
  let to_int64 = Int64.of_int
end

module I16 : S = struct
  type t = int

  let t =
    Repr.map
      (Repr.string_of (`Fixed 2))
      (fun s -> Bytes.get_uint16_le (Bytes.unsafe_of_string s) 0)
      (fun i ->
        let bytes = Bytes.create 2 in
        Bytes.set_uint16_le bytes 0 i ;
        Bytes.unsafe_to_string bytes)

  let byte_size = 2
  let read cstruct offset = Cstruct.HE.get_uint16 cstruct offset
  let write cstruct offset v = Cstruct.HE.set_uint16 cstruct offset v
  let to_string = string_of_int
  let of_int i = i
  let of_int64 = Int64.to_int
  let add t x = t + x
  let succ x = x + 1
  let equal = Int.equal
  let compare = Int.compare
  let to_int64 = Int64.of_int
end

module I32 : S = struct
  type t = Optint.t

  let t =
    Repr.map
      (Repr.string_of (`Fixed 4))
      (fun s -> Optint.decode s ~off:0)
      (fun i ->
        let bytes = Bytes.create 4 in
        Optint.encode bytes ~off:0 i ;
        Bytes.unsafe_to_string bytes)

  let byte_size = 4
  let read cstruct offset = Optint.of_int32 @@ Cstruct.HE.get_uint32 cstruct offset
  let write cstruct offset v = Cstruct.HE.set_uint32 cstruct offset (Optint.to_int32 v)
  let to_string = Optint.to_string
  let of_int i = Optint.of_int i
  let of_int64 = Optint.of_int64
  let add t x = Optint.add t (of_int x)
  let succ = Optint.succ
  let equal = Optint.equal
  let compare = Optint.compare
  let to_int64 = Optint.to_int64
end

module I63 : S = struct
  module Int63 = Optint.Int63

  type t = Int63.t [@@deriving repr]

  let byte_size = 8
  let read cstruct offset = Int63.of_int64 @@ Cstruct.HE.get_uint64 cstruct offset
  let write cstruct offset v = Cstruct.HE.set_uint64 cstruct offset (Int63.to_int64 v)
  let to_string = Int63.to_string
  let of_int i = Int63.of_int i
  let of_int64 i = Int63.of_int64 i
  let add t x = Int63.add t (of_int x)
  let succ = Int63.succ
  let equal = Int63.equal
  let compare = Int63.compare
  let to_int64 x = Int63.to_int64 x
end

module I64 : S = struct
  type t = Int64.t [@@deriving repr]

  let byte_size = 8
  let read cstruct offset = Cstruct.HE.get_uint64 cstruct offset
  let write cstruct offset v = Cstruct.HE.set_uint64 cstruct offset v
  let to_string = Int64.to_string
  let of_int i = Int64.of_int i
  let of_int64 i = i
  let add t x = Int64.add t (of_int x)
  let succ = Int64.succ
  let equal = Int64.equal
  let compare = Int64.compare
  let to_int64 x = x
end

let rec log2 x =
  if Int64.compare x Int64.one <= 0 then 1 else 1 + log2 (Int64.shift_right x 1)

let of_nb_pages nb =
  match log2 nb with
  | bits when bits <= 8 -> (module I8 : S)
  | bits when bits <= 16 -> (module I16 : S)
  | bits when bits <= 32 -> (module I32 : S)
  | bits when bits <= 63 -> (module I63 : S)
  | 64 -> (module I64 : S)
  | bits -> Printf.ksprintf invalid_arg "Disk too large: %i bits" bits
