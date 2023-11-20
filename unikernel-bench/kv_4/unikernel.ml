open Lwt.Syntax

module Main (Block : Mirage_block.S) = struct
  module Block = struct
    include Block

    let discard _ _ = ()
    let flush _ = ()
  end

  let min = 1_000_000
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

  let p_b_s = 16

  module type Formatted_kv = sig
    type t
    type write_error
    type error

    val set : t -> Mirage_kv.Key.t -> string -> (unit, write_error) result Lwt.t
    val get : t -> Mirage_kv.Key.t -> (string, error) result Lwt.t
    val connect : Block.t -> (t, error) result Lwt.t
    val format : Block.t -> (t, write_error) result Lwt.t
    val pp_write_error : Format.formatter -> write_error -> unit
    val pp_error : Format.formatter -> error -> unit
  end

  module Bench (Kv : Formatted_kv) = struct
    let force pp lwt =
      let open Lwt.Infix in
      lwt
      >|= function
      | Ok v -> v
      | Error e ->
        Format.printf "%a@." pp e ;
        failwith "error"

    let format block =
      let+ fs = force Kv.pp_write_error @@ Kv.format block in
      fs

    let bench pp f =
      let cl_start = Int64.to_int (Mclock.elapsed_ns ()) in
      let+ _ = force pp @@ f (Mirage_kv.Key.v "foo") in
      let cl_stop = Int64.to_int (Mclock.elapsed_ns ()) in
      cl_stop - cl_start

    let bench_set fs file_size c =
      bench Kv.pp_write_error (fun key -> Kv.set fs key (String.make file_size c))

    let bench_get fs file_size = bench Kv.pp_error (fun key -> Kv.get fs key)

    let rec n_bench_set acc l n block file_size f =
      if n = 0
      then Lwt.return (acc, l)
      else
        let* fs = format block in
        let* time = f fs file_size in
        n_bench_set (acc + time) (time :: l) (n - 1) block file_size f

    let n_bench_set n block file_size f =
      let+ acc, l = n_bench_set 0 [] n block file_size f in
      median (List.sort compare l)

    let rec n_bench_get acc l n block file_size f =
      if n = 0
      then Lwt.return (acc, l)
      else
        let* fs = force Kv.pp_error @@ Kv.connect block in
        let* time = f fs file_size in
        n_bench_get (acc + time) (time :: l) (n - 1) block file_size f

    let n_bench_get n block file_size f =
      let* fs = format block in
      let* _ =
        (*force @@*) Kv.set fs (Mirage_kv.Key.v "foo") (String.make file_size 'g')
      in
      let+ acc, l = n_bench_get 0 [] n block file_size f in
      median (List.sort compare l)

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

  module Cha = Kv.Make (Block) (Pclock)

  module Bench_cha = Bench (struct
      include Cha

      let format block =
        let* _ = force @@ Cha.format ~program_block_size:p_b_s block in
        let+ fs = force @@ Cha.connect ~program_block_size:p_b_s block in
        Ok fs

      let connect block =
        let+ fs = force @@ Cha.connect ~program_block_size:p_b_s block in
        Ok fs
    end)

  module Fat = Fat.Make (Block)

  module Bench_fat = Bench (struct
      include Fat

      let connect block =
        let+ fs = Fat.connect block in
        Ok fs

      let format block =
        let* info = Block.get_info block in
        let+ fs = Fat.format block info.size_sectors in
        fs

      let set block key str =
        let name = Mirage_kv.Key.to_string key in
        let open Lwt_result.Syntax in
        let* () = Fat.create block name in
        Fat.write block name 0 (Cstruct.of_string str)

      let get block key =
        let name = Mirage_kv.Key.to_string key in
        let* info = force @@ Fat.stat block name in
        let* res = Fat.read block name 0 (Int64.to_int info.size) in
        let str =
          match res with
          | Ok [ cstruct ] -> Cstruct.to_string cstruct
          | _ -> failwith "ERROR"
        in
        Lwt.return (Ok str)
    end)

  let rec init_l l acc max step =
    if acc > max then l else init_l (acc :: l) (acc + step) max step

  let init_fs_size_list min max step = List.rev (init_l [] min max step)

  let start block =
    let l1 = init_fs_size_list 1_000 100_000 5_000 in
    let l2 = init_fs_size_list 1_000_000 10_000_000 500_000 in
    let file_size_l = List.append l1 l2 in
    Format.printf "#CHAMELON@." ;
    let* () = Bench_cha.iterate block file_size_l in
    Format.printf "@.@.#FAT@." ;
    let* () = Bench_fat.iterate block file_size_l in
    let+ () = Block.disconnect block in
    ()
end
