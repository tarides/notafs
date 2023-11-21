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

  (* let read t i l =
     let len = List.length l in
     let i' = Int64.to_int i in
     if i' + len > 6 && i' <= 6
     then
     Fmt.pr "reading sector 6@.";
     read t i l

     let write t i l =
     if List.length l > 5
     then
     (let s = List.nth l 4 in
     Fmt.pr "corrupting sector %a@." Fmt.int64 (Int64.add 4L i);
     Cstruct.set_uint8 s 345 13);
     write t i l *)
end

module Test (Check : Notafs.CHECKSUM) = struct
  module Fs = Notafs.FS (Pclock) (Check) (Block)
  open Lwt.Syntax

  let filename = "myfile"
  let connect () = Block.connect ~prefered_sector_size:(Some sector_size) disk

  let write ~fresh block =
    let* fs = if fresh then Fs.format block else Fs.connect block in
    let t0 = Unix.gettimeofday () in
    let* () =
      match Fs.find fs filename with
      | None -> Lwt.return_unit
      | Some _file -> Fs.remove fs filename
    in
    let* _ = Fs.touch fs filename input_contents in
    let* () = Fs.flush fs in
    let t1 = Unix.gettimeofday () in
    Format.printf "Write: %fs@." (t1 -. t0) ;
    (* Format.printf "%a@." (Repr.pp Notafs.Stats.ro_t) (Fs.stats fs) ; *)
    Lwt.return ()

  let read fs =
    let t0 = Unix.gettimeofday () in
    let file = Option.get @@ Fs.find fs filename in
    let* size = Fs.size file in
    let bytes = Bytes.create size in
    let* _ = Fs.blit_to_bytes file ~off:0 ~len:size bytes in
    let t1 = Unix.gettimeofday () in
    Format.printf "Read: %fs@." (t1 -. t0) ;
    let result = Bytes.unsafe_to_string bytes in
    assert (result = input_contents) ;
    (* Format.printf "%a@." (Repr.pp Notafs.Stats.ro_t) (Fs.stats fs) ; *)
    Lwt.return ()

  let main ~fresh () =
    let* block = connect () in
    let* () = write ~fresh block in
    let* () = Block.disconnect block in
    let* block = connect () in
    (* let* () = write ~fresh:false block in
       let* () = Block.disconnect block in
       let* block = connect () in *)
    let* fs = Fs.connect block in
    let* () = read fs in
    Block.disconnect block

  let main () = Lwt_main.run (main ~fresh:true ())
end

module Test_nocheck = Test (Notafs.No_checksum)
module Test_adler32 = Test (Notafs.Adler32)

let () =
  Format.printf "--- without checksum:@." ;
  Test_nocheck.main () ;
  Format.printf "@." ;
  Format.printf "--- with adler32 checksum:@." ;
  Test_adler32.main ()
