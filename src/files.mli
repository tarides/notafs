module Make (B : Context.A_DISK) : sig
  module Sector : module type of Sector.Make (B)
  module Rope : module type of Rope.Make (B)

  type key = string list
  type file = Rope.t ref
  type t

  val load : Sector.ptr -> (t, B.error) Lwt_result.t
  val verify_checksum : t -> (unit, B.error) Lwt_result.t

  val to_payload
    :  t
    -> Sector.id list
    -> (Rope.t * (Sector.id * Cstruct.t) list * Sector.id list, B.error) Lwt_result.t

  val count_new : t -> (int, B.error) Lwt_result.t
  val exists : t -> key -> [> `Dictionary | `Value ] option
  val find : t -> key -> file
  val find_opt : t -> key -> file option
  val filename : key -> string
  val size : key * file -> (int, B.error) Lwt_result.t
  val add : t -> key -> file -> (t, B.error) Lwt_result.t
  val remove : t -> key -> (t, B.error) Lwt_result.t
  val rename : t -> src:key -> dst:key -> (t, B.error) Lwt_result.t

  val blit_to_bytes
    :  'a * Rope.t ref
    -> off:int
    -> len:int
    -> bytes
    -> (int, B.error) Lwt_result.t

  val blit_from_string
    :  'a
    -> 'b * Rope.t ref
    -> off:int
    -> len:int
    -> string
    -> (unit, B.error) Lwt_result.t

  val reachable_size : t -> (int, B.error) Lwt_result.t
  val list : t -> key -> (string * [> `Dictionary | `Value ]) list
end
