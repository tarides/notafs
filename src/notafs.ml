(* checksum *)
module type CHECKSUM = Checksum.S

module No_checksum = Checksum.No_checksum
module Adler32 = Checksum.Adler32

(* mirage_kv filesystem *)
module KV = Kv.Make

(* lower level filesystem *)
module FS = Fs.Make_check

let get_config = Fs.get_config
