module Make (B : Context.A_DISK) : sig
  module Sector : module type of Sector.Make (B)

  type q = Int64.t * Sector.t

  val make : free_start:Int64.t -> q

  type 'a r := ('a, B.error) Lwt_result.t

  val load : Int64.t * Sector.ptr -> q r
  val push_back : q -> Sector.id list -> q r
  val push_discarded : q -> q r
  val pop_front : q -> int -> (q * Sector.id list) r
  val count_new : q -> int
  val finalize : q -> Sector.id list -> q * Sector.t list
  val allocate : free_queue:q -> Sector.t -> (q * Sector.t list) r
  val self_allocate : free_queue:q -> (q * Sector.t list) r
end
