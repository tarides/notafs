module Writer = struct
  type t =
    { mutable nb_writes : int
    ; mutable sectors_written : int
    ; mutable nb_reads : int
    ; mutable sectors_read : int
    }

  let create () = { nb_writes = 0; nb_reads = 0; sectors_written = 0; sectors_read = 0 }

  let reset t =
    t.nb_writes <- 0 ;
    t.nb_reads <- 0 ;
    t.sectors_written <- 0 ;
    t.sectors_read <- 0

  let incr_write t dv =
    t.nb_writes <- t.nb_writes + 1 ;
    t.sectors_written <- t.sectors_written + dv

  let incr_read t dv =
    t.nb_reads <- t.nb_reads + 1 ;
    t.sectors_read <- t.sectors_read + dv
end

type ro =
  { nb_writes : int
  ; sectors_written : int
  ; nb_reads : int
  ; sectors_read : int
  }
[@@deriving repr]

let snapshot t =
  { nb_writes = t.Writer.nb_writes
  ; nb_reads = t.Writer.nb_reads
  ; sectors_written = t.Writer.sectors_written
  ; sectors_read = t.Writer.sectors_read
  }

include Writer
