module type S = sig
  type bigstring :=
    (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

  type t

  val name : string
  val byte_size : int
  val default : t
  val of_int32 : Int32.t -> t
  val to_int32 : t -> Int32.t
  val equal : t -> t -> bool
  val digest_bigstring : bigstring -> int -> int -> t -> t
  val read : Cstruct.t -> int -> t
  val write : Cstruct.t -> int -> t -> unit
end

module No_checksum : S = struct
  type t = unit

  let name = "NO-CHECK"
  let equal = ( = )
  let default = ()
  let digest_bigstring _ _ _ _ = ()
  let to_int32 _ = Int32.zero
  let of_int32 _ = ()
  let byte_size = 0
  let read _ _ = ()
  let write _ _ _ = ()
end

module Adler32 : S = struct
  include Checkseum.Adler32

  let name = "ADLER-32"
  let byte_size = 4
  let read cstruct offset = of_int32 @@ Cstruct.HE.get_uint32 cstruct offset
  let write cstruct offset v = Cstruct.HE.set_uint32 cstruct offset (to_int32 v)
end
