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
  val get_free_queue : t -> Int64.t * Sector.ptr
  val get_payload : t -> Sector.ptr
  val flush : Sector.t list -> unit io
end = struct
  module Schema = Schema.Make (B)
  module Sector = Schema.Sector
  module Queue = Queue.Make (B)
  open Lwt_result.Syntax

  let nb = 32

  let rec flush = function
    | [] -> Lwt_result.return ()
    | s :: ss ->
      let* () = Sector.write s in
      flush ss

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

  let load () =
    let rec load_gens i acc =
      if i >= nb
      then Lwt_result.return (List.rev acc)
      else
        let* generation = Sector.load_root (Int64.of_int i) in
        load_gens (i + 1) (generation :: acc)
    in
    let+ generations = load_gens 0 [] in
    let generations = Array.of_list generations in
    let generation =
      Array.fold_left (fun g s -> Int64.max g (generation s)) Int64.zero generations
    in
    { generation; generations }

  let format () =
    let free_start = Int64.of_int nb in
    let generation = Int64.of_int (nb + 1) in
    let generations =
      List.init nb (fun i ->
        let i = Int64.of_int i in
        let s = Sector.create ~at:(Sector.root_loc i) () in
        set_generation s i ;
        set_free_start s free_start ;
        set_free_queue s Sector.null_ptr ;
        set_payload s Sector.null_ptr ;
        s)
    in
    let rec write_all = function
      | [] -> Lwt_result.return ()
      | g :: gs ->
        let* () = Sector.write g in
        write_all gs
    in
    let+ () = write_all generations in
    let generations = Array.of_list generations in
    { generation; generations }

  let current_idx t = Int64.rem t.generation (Int64.of_int (Array.length t.generations))
  let current t = t.generations.(Int64.to_int (current_idx t))

  let update t ~queue:(new_free_start, new_free_root) ~payload =
    t.generation <- Int64.succ t.generation ;
    let s = current t in
    set_generation s t.generation ;
    set_free_start s new_free_start ;
    set_free_queue s (Sector.to_ptr new_free_root) ;
    set_payload s (Sector.to_ptr payload) ;
    Sector.write s

  let get_free_queue t =
    let g = current t in
    let queue = free_queue g in
    get_free_start g, queue

  let get_payload t = get_payload (current t)
end
