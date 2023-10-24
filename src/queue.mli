module Make (B : Context.A_DISK) : sig
  module Sector : module type of Sector.Make (B)

  type q = Sector.id * Sector.t
  type 'a r := ('a, B.error) Lwt_result.t

  val make : free_start:Sector.id -> q r
  val load : Sector.id * Sector.ptr -> q r
  val verify_checksum : q -> unit r
  val push_back : q -> Sector.id list -> q r
  val push_discarded : q -> q r
  val pop_front : q -> int -> (q * Sector.id list) r
  val finalize : q -> Sector.id list -> (q * (Sector.id * Cstruct.t) list) r
  val allocate : free_queue:q -> Sector.t -> (q * (Sector.id * Cstruct.t) list) r
  val self_allocate : free_queue:q -> (q * (Sector.id * Cstruct.t) list) r
end
