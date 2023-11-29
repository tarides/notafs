(** Checksum signatures & default implementations. *)

(** Signature for checksum modules. *)
module type S = sig
  type t
  (** Representation of the checksum value. *)

  val name : string
  (** Name of the implementation.

      This will be used to check that the disk used the same algorithm. *)

  val byte_size : int
  (** Number of bytes used by the checksum. *)

  val default : t
  (** Default value of {!t}. *)

  val equal : t -> t -> bool
  (** [equal t1 t2] is the equality between [t1] and [t2]. *)

  val digest : Cstruct.t -> t
  (** [digest cs] computes the checksum of the cstruct [cs]. *)

  val read : Cstruct.t -> int -> t
  (** [read cs off] is the value {!t} read from the cstruct [cs] at offset [off].
      It's guaranteed that at least {!byte_size} bytes are available at this offset. *)

  val write : Cstruct.t -> int -> t -> unit
  (** [write cs off t] writes the value {!t} into the cstruct [cs] at offset [off].
      It's guaranteed that at least {!byte_size} bytes are available at this offset. *)
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
