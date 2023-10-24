(* TODO: Create 100mb file and 1gb disk

   $ dd if=/dev/zero   of=/tmp/large-disk count=2000000
   $ dd if=/dev/random of=/tmp/large-file count=200000
*)

let disk = "/tmp/large-disk"
let input = "/tmp/large-file"
let sector_size = 4096

let input_contents =
  let t0 = Unix.gettimeofday () in
  let h = open_in input in
  let len = in_channel_length h in
  let bytes = Bytes.create len in
  let rec go i =
    let chunk = min sector_size (len - i) in
    if chunk = 0
    then ()
    else (
      let quantity = Stdlib.input h bytes i chunk in
      go (i + quantity))
  in
  go 0 ;
  close_in h ;
  let result = Bytes.unsafe_to_string bytes in
  let t1 = Unix.gettimeofday () in
  Format.printf
    "Unix read in %fs, %#i bytes, %#i sectors@."
    (t1 -. t0)
    (String.length result)
    (String.length result / sector_size) ;
  result

module Block = struct
  include Block

  let discard _ _ = ()
  let flush _ = ()
  let nb_writes = ref 0
  let sectors_written = ref 0

  let write t i cs =
    incr nb_writes ;
    sectors_written := !sectors_written + List.length cs ;
    write t i cs

  let nb_reads = ref 0
  let sectors_read = ref 0

  let read t i cs =
    incr nb_reads ;
    sectors_read := !sectors_read + List.length cs ;
    read t i cs

  let stats () =
    Format.printf
      "nb_writes = %#i (%#i sectors written), nb_reads = %#i (%#i sectors read)@."
      !nb_writes
      !sectors_written
      !nb_reads
      !sectors_read ;
    nb_writes := 0 ;
    nb_reads := 0 ;
    sectors_written := 0 ;
    sectors_read := 0
end

open Lwt.Syntax

let no_error lwt =
  Lwt.map
    (function
     | Ok v -> v
     | Error _ -> failwith "unexpected error")
    lwt

module Test (Kv : Mirage_kv.RW) = struct
  let filename = Mirage_kv.Key.v "test"

  let write fs =
    let t0 = Unix.gettimeofday () in
    let+ () = no_error @@ Kv.set fs filename input_contents in
    let t1 = Unix.gettimeofday () in
    Format.printf "Write: %fs@." (t1 -. t0) ;
    Block.stats ()

  let read fs =
    let t0 = Unix.gettimeofday () in
    let+ contents = no_error @@ Kv.get fs filename in
    let t1 = Unix.gettimeofday () in
    Format.printf "Read: %fs@." (t1 -. t0) ;
    assert (contents = input_contents) ;
    Block.stats ()

  let main fs =
    let* () = write fs in
    let* () = write fs in
    let* () = write fs in
    let* () = write fs in
    let* () = write fs in
    let* () = write fs in
    let* () = write fs in
    let* () = write fs in
    let* () = write fs in
    let* () = read fs in
    let* () = read fs in
    let* () = write fs in
    let* () = read fs in
    Lwt.return_unit
end

module Notafs_kv = Notafs.KV (Block)
module Test_notafs = Test (Notafs_kv)
module Notafs_kv_crc = Notafs.Make_kv (Notafs.Adler32) (Block)
module Test_notafs_crc = Test (Notafs_kv_crc)
module Tar_kv = Tar_mirage.Make_KV_RW (Pclock) (Block)
module Test_tar = Test (Tar_kv)

let reset block =
  let zero = Cstruct.create sector_size in
  no_error @@ Block.write block Int64.zero (List.init 16 (fun _ -> zero))

let main () =
  let* block = Block.connect ~prefered_sector_size:(Some sector_size) disk in
  let* () =
    let* fs = no_error @@ Notafs_kv.format block in
    Format.printf "@.--- Notafs without checksum:@." ;
    let* () = Test_notafs.main fs in
    Format.printf "%a@." (Repr.pp Notafs.Stats.ro_t) (Notafs_kv.stats fs) ;
    Lwt.return ()
  in
  let* () =
    let* fs = no_error @@ Notafs_kv_crc.format block in
    Format.printf "@.--- Notafs with checksum:@." ;
    let* () = Test_notafs_crc.main fs in
    Format.printf "%a@." (Repr.pp Notafs.Stats.ro_t) (Notafs_kv_crc.stats fs) ;
    Lwt.return ()
  in
  let* () = reset block in
  let* () =
    let* fs = Tar_kv.connect block in
    Format.printf "@.--- Tar:@." ;
    let* () = Test_tar.main fs in
    Lwt.return ()
  in
  Lwt.return ()

let () = Lwt_main.run (main ())
