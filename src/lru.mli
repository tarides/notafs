type 'a t
type 'a elt

val make : unit -> 'a t
val make_elt : 'a -> 'a t -> 'a elt
val make_detached : 'a -> 'a elt
val use : 'a elt -> 'a t -> unit
val pop_back : 'a t -> 'a option
val detach : 'a elt -> 'a t -> unit
val value : 'a elt -> 'a
val length : 'a t -> int
