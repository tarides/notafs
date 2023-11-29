module type CHECKSUM = Checksum.S

module No_checksum = Checksum.No_checksum
module Adler32 = Checksum.Adler32
module KV = Kv.Make
module FS = Fs.Make_check

type config = Header.config =
  { disk_size : Int64.t
  ; page_size : int
  ; checksum_algorithm : string
  ; checksum_byte_size : int
  }

let get_config = Fs.get_config
