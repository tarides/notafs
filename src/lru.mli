type 'a t
type 'a elt

val make : unit -> 'a t
val make_elt : 'a -> 'a t -> 'a elt
val make_detached : 'a -> 'a elt
val use : 'a elt -> 'a t -> unit
val peek_back : 'a t -> 'a option
val pop_back : 'a t -> 'a option
val detach : 'a elt -> 'a t -> unit
val detach_remove : 'a elt -> 'a t -> unit
val value : 'a elt -> 'a
val length : 'a t -> int
val iter : ('a -> unit) -> 'a t -> unit
val clear : 'a t -> unit
