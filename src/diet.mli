module type S = sig
  type t

  val compare : t -> t -> int
  val to_string : t -> string
  val add : t -> int -> t
end

module Make (Id : S) : sig
  type t

  val empty : t
  val add : t -> Id.t -> t
  val add_range : t -> Id.t * int -> t
  val to_list : t -> Id.t list
  val of_list : Id.t list -> t
  val to_range_list : t -> (Id.t * int) list
end
