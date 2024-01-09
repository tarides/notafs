module Make (B : Context.A_DISK) : sig
  module Sector : module type of Sector.Make (B)

  type range = Sector.id * int

  type q =
    { free_start : Sector.id
    ; free_queue : Sector.t
    ; bitset : Sector.t
    ; bitset_start : Sector.id
    ; free_sectors : Int64.t
    }

  type 'a r := ('a, B.error) Lwt_result.t

  val load : Sector.id * Sector.ptr * Sector.ptr * Sector.id * Int64.t -> q r
  val verify_checksum : q -> unit r
  val push_back : q -> range list -> q r
  val push_discarded : q -> q r
  val pop_front : q -> int -> (q * range list) r
  val finalize : q -> Sector.id list -> (q * (Sector.id * Cstruct.t) list) r
  val allocate : free_queue:q -> Sector.t -> (q * (Sector.id * Cstruct.t) list) r
  val self_allocate : free_queue:q -> (q * (Sector.id * Cstruct.t) list) r
  val size : q -> int r
  val reachable_size : q -> int r
end
