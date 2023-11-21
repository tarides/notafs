open Lwt.Syntax

module Main (Block : Mirage_block.S) (Doc : Mirage_kv.RO) = struct
  let min = 1_000
  let max = 10_000_000
  let step = 500_000
  let nb_run = 10

  let force lwt =
    let open Lwt.Infix in
    lwt
    >|= function
    | Ok v -> v
    | Error _ -> failwith "error"

  let median sorted_l =
    let len = List.length sorted_l in
    if len mod 2 = 0
    then (List.nth sorted_l (len / 2) + List.nth sorted_l ((len / 2) + 1)) / 2
    else List.nth sorted_l ((len + 1) / 2)

  let pp_perf acc n l =
    let l = List.sort compare l in
    let min = List.nth l 0 in
    let max = List.nth l (List.length l - 1) in
    Printf.sprintf "{min: %#d; max: %#d; avg: %#d; med: %#d}" min max (acc / n) (median l)

  module type Formatted_kv = sig
    include Mirage_kv.RW

    val connect : Block.t -> (t, error) result Lwt.t
    val format : Block.t -> (t, write_error) result Lwt.t
  end

  module Bench (Kv : Formatted_kv) = struct
    let format block =
      let+ fs = force @@ Kv.format block in
      fs

    let bench f =
      let cl_start = Int64.to_int (Mclock.elapsed_ns ()) in
      let+ _ = force @@ f (Mirage_kv.Key.v "foo") in
      let cl_stop = Int64.to_int (Mclock.elapsed_ns ()) in
      cl_stop - cl_start

    let bench_set fs file_size c =
      bench (fun key -> Kv.set fs key (String.make file_size c))

    let bench_get fs file_size = bench (fun key -> Kv.get fs key)

    let rec n_bench_set acc l n block file_size f =
      if n = 0
      then Lwt.return (acc, l)
      else
        let* fs = format block in
        let* time = f fs file_size in
        n_bench_set (acc + time) (time :: l) (n - 1) block file_size f

    let n_bench_set n block file_size f =
      let* acc, l = n_bench_set 0 [] n block file_size f in
      Lwt.return (median (List.sort compare l))

    let rec n_bench_get acc l n block file_size f =
      if n = 0
      then Lwt.return (acc, l)
      else
        let* fs = force @@ Kv.connect block in
        let* time = f fs file_size in
        n_bench_get (acc + time) (time :: l) (n - 1) block file_size f

    let n_bench_get n block file_size f =
      let* fs = format block in
      let* () = force @@ Kv.set fs (Mirage_kv.Key.v "foo") (String.make file_size 'g') in
      let* acc, l = n_bench_get 0 [] n block file_size f in
      Lwt.return (median (List.sort compare l))

    let rec iterate block file_size_l =
      if file_size_l = []
      then Lwt.return ()
      else (
        let file_size = List.hd file_size_l in
        let* mediane_set =
          n_bench_set nb_run block file_size (fun fs file_size ->
            bench_set fs file_size 'n')
        in
        let* mediane_get =
          n_bench_get nb_run block file_size (fun fs file_size -> bench_get fs file_size)
        in
        Format.printf "%d\t%d\t%d@." file_size mediane_set mediane_get ;
        iterate block (List.tl file_size_l))
  end

  module type Formatted_kv_RO = Mirage_kv.RO

  module Bench_RO (Kv : Formatted_kv_RO) = struct
    let force lwt =
      let open Lwt.Infix in
      lwt
      >|= function
      | Ok v -> v
      | Error e ->
        Format.printf "%a@." Kv.pp_error e ;
        failwith "error"

    let bench f name =
      let cl_start = Int64.to_int (Mclock.elapsed_ns ()) in
      let+ res = force @@ f (Mirage_kv.Key.v name) in
      let cl_stop = Int64.to_int (Mclock.elapsed_ns ()) in
      cl_stop - cl_start

    let bench_get store name = bench (fun key -> Kv.get store key) name

    let rec n_bench_get acc l n store f =
      if n = 0
      then Lwt.return (acc, l)
      else
        let* time = f store in
        n_bench_get (acc + time) (time :: l) (n - 1) store f

    let n_bench_get n store f =
      let* acc, l = n_bench_get 0 [] n store f in
      Lwt.return (median (List.sort compare l))

    let rec iterate store file_size_l =
      if file_size_l = []
      then Lwt.return ()
      else (
        let file_size = List.hd file_size_l in
        let* mediane_get =
          n_bench_get nb_run store (fun store ->
            bench_get store (string_of_int file_size))
        in
        Format.printf "%d\t%d@." file_size mediane_get ;
        iterate store (List.tl file_size_l))
  end

  module Notaf = Notafs.KV (Notafs.No_checksum) (Block)
  module Tar = Tar_mirage.Make_KV_RW (Pclock) (Block)
  module Bench_notaf = Bench (Notaf)

  module Bench_tar = Bench (struct
      include Tar

      let format block =
        let* info = Block.get_info block in
        let* () =
          force @@ Block.write block Int64.zero [ Cstruct.create (info.sector_size * 2) ]
        in
        let+ fs = Tar.connect block in
        Ok fs

      let connect block =
        let* fs = Tar.connect block in
        Lwt.return (Ok fs)
    end)

  module Bench_doc = Bench_RO (Doc)

  let rec init_l l acc max step =
    if acc > max then l else init_l (acc :: l) (acc + step) max step

  let init_fs_size_list min max step = List.rev (init_l [] min max step)

  let start block store =
    let l1 = init_fs_size_list 1_000 100_000 5_000 in
    let l2 = init_fs_size_list 1_000_000 10_000_000 500_000 in
    let file_size_l = List.append l1 l2 in
    Format.printf "#NOTAFS@." ;
    let* () = Bench_notaf.iterate block file_size_l in
    Format.printf "@.@.#TAR@." ;
    let* () = Bench_tar.iterate block file_size_l in
    Format.printf "@.@.#DOCTEUR@." ;
    let* () = Bench_doc.iterate store file_size_l in
    let+ () = Block.disconnect block in
    ()
end
