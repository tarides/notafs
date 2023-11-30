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
  val to_list : t -> Id.t list
end
