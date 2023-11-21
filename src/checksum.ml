module type S = sig
  type t

  val name : string
  val byte_size : int
  val default : t
  val equal : t -> t -> bool
  val digest : Cstruct.t -> t
  val read : Cstruct.t -> int -> t
  val write : Cstruct.t -> int -> t -> unit
end

module No_checksum : S = struct
  type t = unit

  let name = "NO-CHECK"
  let byte_size = 0
  let default = ()
  let equal = ( = )
  let digest _ = ()
  let read _ _ = ()
  let write _ _ _ = ()
end

module Adler32 : S = struct
  include Checkseum.Adler32

  let name = "ADLER-32"
  let byte_size = 4

  let digest cstruct =
    digest_bigstring (Cstruct.to_bigarray cstruct) 0 (Cstruct.length cstruct) default

  let read cstruct offset = of_int32 @@ Cstruct.HE.get_uint32 cstruct offset
  let write cstruct offset v = Cstruct.HE.set_uint32 cstruct offset (to_int32 v)
end
