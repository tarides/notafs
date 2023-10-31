let notafs_magic = 0x534641544F4EL (* NOTAFS *)

module Leaf (B : Context.A_DISK) : sig
  module Schema : module type of Schema.Make (B)
  module Sector = Schema.Sector
  module Queue : module type of Queue.Make (B)

  type t = Sector.t
  type 'a io := ('a, B.error) Lwt_result.t
  type q := Sector.id * Sector.ptr * int64

  val get_magic : t -> int64 io
  val get_free_queue : t -> q io
  val get_payload : t -> Sector.ptr io
  val generation : t -> int64 io
  val create : gen:int64 -> at:B.Id.t -> q -> Sector.ptr -> Sector.t io
end = struct
  module Schema = Schema.Make (B)
  module Sector = Schema.Sector
  module Queue = Queue.Make (B)
  open Lwt_result.Syntax

  type t = Sector.t

  type schema =
    { magic : int64 Schema.field
    ; generation : int64 Schema.field
    ; free_start : Sector.id Schema.field
    ; free_queue : Schema.ptr
    ; free_sectors : int64 Schema.field
    ; payload : Schema.ptr
    }

  let { magic; generation; free_start; free_queue; free_sectors; payload } =
    Schema.define
    @@
    let open Schema.Syntax in
    let+ magic = Schema.uint64
    and+ generation = Schema.uint64
    and+ free_start = Schema.id
    and+ free_queue = Schema.ptr
    and+ free_sectors = Schema.uint64
    and+ payload = Schema.ptr in
    { magic; generation; free_start; free_queue; free_sectors; payload }

  include struct
    open Schema.Infix

    let set_magic t v = t.@(magic) <- v
    let get_magic t = t.@(magic)
    let set_generation t v = t.@(generation) <- v
    let generation t = t.@(generation)
    let set_free_start t v = t.@(free_start) <- v
    let get_free_start t = t.@(free_start)
    let set_free_queue t v = t.@(free_queue) <- v
    let free_queue t = t.@(free_queue)
    let set_free_sectors t v = t.@(free_sectors) <- v
    let free_sectors t = t.@(free_sectors)
    let get_payload t = t.@(payload)
    let set_payload t v = t.@(payload) <- v
  end

  let get_free_queue t =
    let* queue = free_queue t in
    let* free_start = get_free_start t in
    let+ free_sectors = free_sectors t in
    free_start, queue, free_sectors

  let create ~gen ~at (free_start, free_queue, free_sectors) payload =
    let* s = Sector.create ~at:(Sector.root_loc at) () in
    let* () = set_magic s notafs_magic in
    let* () = set_generation s gen in
    let* () = set_free_start s free_start in
    let* () = set_free_queue s free_queue in
    let* () = set_free_sectors s free_sectors in
    let+ () = set_payload s payload in
    s
end

module Make (B : Context.A_DISK) = struct
  module Leaf = Leaf (B)
  module Schema = Leaf.Schema
  module Sector = Leaf.Sector
  module Queue = Leaf.Queue
  open Lwt_result.Syntax

  let rec regroup (first, last, cs, acc) = function
    | [] -> List.rev ((first, List.rev cs) :: acc)
    | (id, c) :: rest ->
      if B.Id.(equal (succ last) id)
      then regroup (first, id, c :: cs, acc) rest
      else regroup (id, id, [ c ], (first, List.rev cs) :: acc) rest

  let regroup = function
    | [] -> []
    | (id, c) :: rest -> regroup (id, id, [ c ], []) rest

  let regroup lst =
    regroup @@ List.sort (fun (a_id, _) (b_id, _) -> B.Id.compare a_id b_id) lst

  let rec flush = function
    | [] -> Lwt_result.return ()
    | (id, cs) :: css ->
      let id_ = Int64.to_int @@ B.Id.to_int64 id in
      assert (id_ <> 0 && id_ <> 1) ;
      let* () = B.write id cs in
      flush css

  let flush lst =
    let lst = regroup lst in
    flush lst

  type t =
    { mutable generation : Int64.t
    ; mutable current : Leaf.t
    ; mutable parent_at : int
    ; mutable parent : Sector.t
    ; mutable current_idx : int
    ; mutable kept : Sector.id list
    }

  type schema =
    { first_generation : int64 Schema.field
    ; generations : Schema.id Schema.dyn_array
    }

  let { first_generation; generations } =
    Schema.define
    @@
    let open Schema.Syntax in
    let+ first_generation = Schema.uint64
    and+ generations = Schema.array Schema.id in
    { first_generation; generations }

  let rec find_latest_generation i g acc = function
    | [] -> i, g, acc
    | (is, gen, s) :: ss ->
      let i, g, acc = if Int64.compare g gen > 0 then i, g, acc else is, gen, s in
      find_latest_generation i g acc ss

  let find_latest_generation = function
    | [] -> None
    | (i, g, s) :: ss ->
      let r = find_latest_generation i g s ss in
      Some r

  let nb = 8
  let nb_kept = 4

  let rec split_at n acc = function
    | rest when n = 0 -> List.rev acc, rest
    | x :: xs -> split_at (n - 1) (x :: acc) xs
    | [] -> List.rev acc, []

  let kept generations =
    fst
    @@ split_at nb_kept []
    @@ List.map (fun (_, _, s) -> Sector.force_id s)
    @@ List.sort (fun (_, a, _) (_, b, _) -> Int64.compare b a) generations

  let load () =
    let* s0 = Sector.load_root (B.Id.of_int 0) in
    let* s1 = Sector.load_root (B.Id.of_int 1) in
    let open Schema.Infix in
    let* nb0 = s0.@(generations.length) in
    let* f0 = s0.@(first_generation) in
    let* nb1 = s1.@(generations.length) in
    let* f1 = s1.@(first_generation) in
    let rec load_gens nb s i expected_gen acc =
      if i >= nb
      then Lwt_result.return acc
      else
        let* at = s.@(Schema.nth generations i) in
        Lwt.bind
          (let* generation = Sector.load_root at in
           let* magic = Leaf.get_magic generation in
           let* () =
             if magic <> notafs_magic
             then Lwt_result.fail `Disk_not_formatted
             else Lwt_result.return ()
           in
           let* g = Leaf.generation generation in
           let+ () =
             if expected_gen <> g
             then Lwt_result.fail `Disk_not_formatted
             else Lwt_result.return ()
           in
           g, generation)
          (function
           | Ok (g, generation) ->
             load_gens nb s (i + 1) (Int64.succ expected_gen) ((i, g, generation) :: acc)
           | Error _ -> load_gens nb s (i + 1) (Int64.succ expected_gen) acc)
    in
    let* generations_s0 = load_gens nb0 s0 0 f0 [] in
    let* generations_s1 = load_gens nb1 s1 0 f1 [] in
    let opt0 = find_latest_generation generations_s0 in
    let opt1 = find_latest_generation generations_s1 in
    match opt0, opt1 with
    | None, None -> Lwt_result.fail `Disk_not_formatted
    | Some (idx0, generation_s0, current_s0), None ->
      Lwt_result.return
        { generation = generation_s0
        ; current = current_s0
        ; parent_at = 0
        ; parent = s0
        ; current_idx = idx0
        ; kept = kept generations_s0
        }
    | None, Some (idx1, generation_s1, current_s1) ->
      Lwt_result.return
        { generation = generation_s1
        ; current = current_s1
        ; parent_at = 1
        ; parent = s1
        ; current_idx = idx1
        ; kept = kept generations_s1
        }
    | Some (idx0, generation_s0, current_s0), Some (idx1, generation_s1, current_s1) ->
      Lwt_result.return
        (if generation_s0 > generation_s1
         then
           { generation = generation_s0
           ; current = current_s0
           ; parent_at = 0
           ; parent = s0
           ; current_idx = idx0
           ; kept = kept (generations_s0 @ generations_s1)
           }
         else
           { generation = generation_s1
           ; current = current_s1
           ; parent_at = 1
           ; parent = s1
           ; current_idx = idx1
           ; kept = kept (generations_s1 @ generations_s0)
           })

  let format () =
    let free_start = B.Id.of_int (nb + 2) in
    let free_sectors = Int64.sub B.nb_sectors (Int64.of_int (nb + 2)) in
    let* s0 = Sector.create ~at:(Sector.root_loc @@ B.Id.of_int 0) () in
    let* s1 = Sector.create ~at:(Sector.root_loc @@ B.Id.of_int 1) () in
    let open Schema.Infix in
    let* () = s0.@(generations.length) <- nb in
    let* () = s0.@(first_generation) <- Int64.zero in
    let* () = s1.@(generations.length) <- 0 in
    let rec go i =
      if i >= nb
      then Lwt_result.return ()
      else begin
        let* () = s0.@(Schema.nth generations i) <- B.Id.of_int (i + 2) in
        (* we probably want to reset the sectors? *)
        go (i + 1)
      end
    in
    let* () = go 0 in
    let* at = s0.@(Schema.nth generations 0) in
    let* first =
      Leaf.create
        ~gen:Int64.zero
        ~at
        (free_start, Sector.null_ptr, free_sectors)
        Sector.null_ptr
    in
    let rec write_fakes i =
      if i >= nb
      then Lwt_result.return ()
      else
        let* fake = Sector.create ~at:(Sector.root_loc @@ B.Id.of_int (i + 2)) () in
        let* () = Sector.write_root fake in
        write_fakes (i + 1)
    in
    let* () = write_fakes 1 in
    let* () = Sector.write_root first in
    let* () = Sector.write_root s0 in
    let+ () = Sector.write_root s1 in
    { generation = Int64.zero
    ; current = first
    ; parent_at = 0
    ; parent = s0
    ; current_idx = 0
    ; kept = [ Sector.force_id first ]
    }

  let current_idx t = Int64.rem t.generation (Int64.of_int nb)

  let update t ~queue ~payload =
    t.generation <- Int64.succ t.generation ;
    t.current_idx <- t.current_idx + 1 ;
    let open Schema.Infix in
    let* max_gens = t.parent.@(generations.length) in
    let* queue =
      if t.current_idx < max_gens
      then Lwt_result.return queue
      else begin
        t.parent_at <- (t.parent_at + 1) mod 2 ;
        let* s1 = Sector.create ~at:(Sector.root_loc @@ B.Id.of_int t.parent_at) () in
        t.parent <- s1 ;
        let* queue, free_gens = Queue.pop_front queue nb in
        let* () = s1.@(generations.length) <- nb in
        let* () = s1.@(first_generation) <- t.generation in
        let rec go i = function
          | [] ->
            assert (i = nb) ;
            Lwt_result.return ()
          | g :: gens ->
            let* () = s1.@(Schema.nth generations i) <- g in
            go (i + 1) gens
        in
        let* () = go 0 free_gens in
        let+ () = Sector.write_root s1 in
        t.current_idx <- 0 ;
        t.parent <- s1 ;
        queue
      end
    in
    let* at = t.parent.@(Schema.nth generations t.current_idx) in
    let* queue =
      let kept, rest = split_at nb_kept [] (at :: t.kept) in
      t.kept <- kept ;
      match rest with
      | [] -> Lwt_result.return queue
      | [ to_remove ] ->
        let* ss = Sector.load_root to_remove in
        let* g = Leaf.generation ss in
        assert (Int64.add g (Int64.of_int nb_kept) = t.generation) ;
        let* queue = Queue.push_back queue [ to_remove ] in
        let* queue, to_flush_queue = Queue.self_allocate ~free_queue:queue in
        let+ () = flush to_flush_queue in
        t.kept <- kept ;
        queue
      | _ -> assert false
    in
    let { Queue.free_start = new_free_start
        ; free_queue = new_free_root
        ; free_sectors = new_free_sectors
        }
      =
      queue
    in
    let* current =
      Leaf.create
        ~gen:t.generation
        ~at
        (new_free_start, Sector.to_ptr new_free_root, new_free_sectors)
        (Sector.to_ptr payload)
    in
    t.current <- current ;
    let+ () = Sector.write_root current in
    queue

  let get_free_queue t = Leaf.get_free_queue t.current
  let get_payload t = Leaf.get_payload t.current

  let pred_gen t =
    let _ = failwith "todo: update current!" in
    t.generation <- Int64.pred t.generation

  let reachable_size t =
    let reserved = nb - 1 - t.current_idx in
    2 + List.length t.kept + reserved
end
