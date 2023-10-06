type t

val create : unit -> t
val reset : t -> unit
val incr_write : t -> int -> unit
val incr_read : t -> int -> unit

type ro =
  { nb_writes : int
  ; sectors_written : int
  ; nb_reads : int
  ; sectors_read : int
  }
[@@deriving repr]

val snapshot : t -> ro
