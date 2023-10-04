module Make (B : Context.A_DISK) : sig
  module Schema : module type of Schema.Make (B)
  module Sector = Schema.Sector
  module Queue : module type of Queue.Make (B)

  type t
  type 'a io := ('a, B.error) Lwt_result.t

  val nb : int
  val load : unit -> t io
  val format : unit -> t io
  val update : t -> queue:Queue.q -> payload:Sector.t -> unit io
  val get_free_queue : t -> (Int64.t * Sector.ptr) io
  val get_payload : t -> Sector.ptr io
  val flush : Sector.t list -> unit io
end = struct
  module Schema = Schema.Make (B)
  module Sector = Schema.Sector
  module Queue = Queue.Make (B)
  open Lwt_result.Syntax

  let nb = 2

  let rec regroup (first, last, cs, acc) = function
    | [] -> List.rev ((first, List.rev cs) :: acc)
    | (id, c) :: rest ->
      if Int64.(equal (succ last) id)
      then regroup (first, id, c :: cs, acc) rest
      else regroup (id, id, [ c ], (first, List.rev cs) :: acc) rest

  let regroup = function
    | [] -> invalid_arg "Root.regroup: empty list"
    | (id, c) :: rest -> regroup (id, id, [ c ], []) rest

  let rec list_to_write acc = function
    | [] -> Lwt_result.return acc
    | x :: xs ->
      let* w = Sector.to_write x in
      list_to_write (w :: acc) xs

  let regroup lst =
    let+ lst = list_to_write [] lst in
    regroup @@ List.sort (fun (a_id, _) (b_id, _) -> Int64.compare a_id b_id) lst

  let rec flush = function
    | [] -> Lwt_result.return ()
    | (id, cs) :: css ->
      let* () = B.write id cs in
      flush css

  let flush lst =
    let* lst = regroup lst in
    flush lst

  type schema =
    { generation : int64 Schema.field
    ; free_start : int64 Schema.field
    ; free_queue : Schema.ptr
    ; payload : Schema.ptr
    }

  let { generation; free_start; free_queue; payload } =
    Schema.define
    @@
    let open Schema.Syntax in
    let+ generation = Schema.uint64
    and+ free_start = Schema.uint64
    and+ free_queue = Schema.ptr
    and+ payload = Schema.ptr in
    { generation; free_start; free_queue; payload }

  include struct
    open Schema.Infix

    let set_generation t v = t.@(generation) <- v
    let generation t = t.@(generation)
    let set_free_start t v = t.@(free_start) <- v
    let get_free_start t = t.@(free_start)
    let set_free_queue t v = t.@(free_queue) <- v
    let free_queue t = t.@(free_queue)
    let get_payload t = t.@(payload)
    let set_payload t v = t.@(payload) <- v
  end

  type t =
    { mutable generation : Int64.t
    ; generations : Sector.t array
    }

  let rec find_latest_generation g = function
    | [] -> Lwt_result.return g
    | s :: ss ->
      let* gen = generation s in
      find_latest_generation (Int64.max g gen) ss

  let load () =
    let rec load_gens i acc =
      if i >= nb
      then Lwt_result.return (List.rev acc)
      else
        let* generation = Sector.load_root (Int64.of_int i) in
        load_gens (i + 1) (generation :: acc)
    in
    let* generations = load_gens 0 [] in
    let+ generation = find_latest_generation Int64.zero generations in
    let generations = Array.of_list generations in
    { generation; generations }

  let format () =
    let free_start = Int64.of_int nb in
    let generation = Int64.of_int 0 in
    let* generations =
      let create_gen i =
        let i = Int64.of_int i in
        let* s = Sector.create ~at:(Sector.root_loc i) () in
        let* () = set_generation s Int64.zero in
        let* () = set_free_start s free_start in
        let* () = set_free_queue s Sector.null_ptr in
        let+ () = set_payload s Sector.null_ptr in
        s
      in
      let rec create_gens i acc =
        if i >= nb
        then Lwt_result.return (List.rev acc)
        else
          let* g = create_gen i in
          create_gens (i + 1) (g :: acc)
      in
      create_gens 0 []
    in
    let+ () = flush generations in
    let generations = Array.of_list generations in
    { generation; generations }

  let current_idx t = Int64.rem t.generation (Int64.of_int (Array.length t.generations))
  let current t = t.generations.(Int64.to_int (current_idx t))

  let update t ~queue:(new_free_start, new_free_root) ~payload =
    t.generation <- Int64.succ t.generation ;
    let s = current t in
    let* () = set_generation s t.generation in
    let* () = set_free_start s new_free_start in
    let* () = set_free_queue s (Sector.to_ptr new_free_root) in
    let* () = set_payload s (Sector.to_ptr payload) in
    let* id, cstruct = Sector.to_write s in
    B.write id [ cstruct ]

  let get_free_queue t =
    let g = current t in
    let* queue = free_queue g in
    let+ free_start = get_free_start g in
    free_start, queue

  let get_payload t = get_payload (current t)
end
