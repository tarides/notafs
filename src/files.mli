module Make (Clock : Mirage_clock.PCLOCK) (B : Context.A_DISK) : sig
  module Sector : module type of Sector.Make (B)
  module Rope : module type of Rope.Make (B)

  type key = string list
  type file
  type t
  type 'a io := ('a, B.error) Lwt_result.t

  val load : Sector.ptr -> t io
  val verify_checksum : t -> unit io

  (* *)
  val exists : t -> key -> [> `Dictionary | `Value ] option
  val last_modified : t -> key -> Ptime.t
  val find_opt : t -> key -> file option
  val filename : key -> string
  val size : file -> int io
  val add : t -> key -> file -> t io
  val remove : t -> key -> t io
  val rename : t -> src:key -> dst:key -> t io
  val touch : t -> key -> string -> (t * file) io
  val blit_to_bytes : t -> file -> off:int -> len:int -> bytes -> int io
  val blit_from_string : t -> file -> off:int -> len:int -> string -> unit io
  val append_from : t -> file -> string * int * int -> unit io
  val list : t -> key -> (string * [> `Dictionary | `Value ]) list

  (* *)

  val count_new : t -> int io

  val to_payload
    :  t
    -> Sector.id list
    -> (Rope.t * (Sector.id * Cstruct.t) list * Sector.id list) io

  val reachable_size : t -> int io
end
