(* stats *)
module Stats = Stats

(* interfaces *)

module type DISK = Main.DISK
module type CHECKSUM = Main.CHECKSUM

(* default checkseum impl *)
module No_checksum = Main.No_checksum
module Adler32 = Main.Adler32

(* main functors *)
module Make_disk = Main.Make_disk
module Make_check = Main.Make_check
module Make = Main.Make
module KV = Kv.Make

let metadatas = Main.metadatas
