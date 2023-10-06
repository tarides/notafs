module Make (B : Mirage_block.S) : sig
  include Notafs.DISK

  val draw_status : t -> string -> unit
  val of_block : B.t -> t Lwt.t
  val sleep : float ref
  val pause : bool ref
end = struct
  type bitmap = int array array

  type t =
    { block : B.t
    ; mutable size : int * int
    ; nb_pages : int
    ; bitmaps : bitmap array
    ; write_count : int array
    ; read_count : int array
    }

  type error = B.error
  type write_error = B.write_error

  let pp_error = B.pp_error
  let pp_write_error = B.pp_write_error
  let get_info t = B.get_info t.block

  let read t i cs =
    List.iteri
      (fun j _ ->
        let at = Int64.to_int i + j in
        t.read_count.(at) <- t.read_count.(at) + 1)
      cs ;
    B.read t.block i cs

  module G = Graphics

  let get_color i = Color.of_hsl (float i *. 360.0 /. 256.) 0.6 0.6

  let colors =
    Array.init 256
    @@ fun i ->
    let c = get_color i in
    let r, g, b = Gg.V4.x c, Gg.V4.y c, Gg.V4.z c in
    let i v = max 0 @@ min 255 @@ int_of_float (255.0 *. v) in
    let g = G.rgb (i r) (i g) (i b) in
    assert (g >= 0 && g <= 0xFFFFFF) ;
    assert (g <> G.transp) ;
    g

  let factor = 1
  let true_size = 32
  let size = true_size * factor
  let border = 2
  let border_bottom = 100
  let size' = size + border

  let draw_status t msg : unit =
    let w, _ = t.size in
    (* let w, h = G.text_size msg in *)
    G.set_color G.white ;
    G.fill_rect 0 (border_bottom - 15) w 15 ;
    G.set_color G.black ;
    G.moveto 10 (border_bottom - 15) ;
    G.draw_string msg

  let draw_image img x y =
    let img = G.make_image img in
    G.draw_image img x y

  let refresh t =
    let ((w, _) as size) = Graphics.size_x (), Graphics.size_y () in
    if t.size <> size
    then begin
      t.size <- size ;
      G.clear_graph () ;
      for i = 0 to Array.length t.bitmaps - 1 do
        let nb_horz = (w - border) / size' in
        let x, y = i mod nb_horz, i / nb_horz in
        let x, y = border + (x * size'), border_bottom + (y * size') in
        let img = t.bitmaps.(i) in
        draw_image img x y
      done
    end

  let refresh_write_count t =
    let worst = Array.fold_left max 0 t.write_count in
    let worst = max 10 worst in
    let s = 4 in
    G.set_color G.black ;
    for i = 0 to Array.length t.write_count - 1 do
      let height = 32 * t.write_count.(i) / worst in
      G.fill_rect (s * i) 0 s height
    done

  let refresh_read_count t =
    let worst = Array.fold_left max 0 t.read_count in
    let worst = max 10 worst in
    let wrote = ref false in
    let s = 4 in
    G.set_color G.black ;
    for i = 0 to Array.length t.read_count - 1 do
      let height = 32 * t.read_count.(i) / worst in
      G.fill_rect (s * i) 34 s height ;
      if (not !wrote) && t.read_count.(i) = worst
      then begin
        wrote := true ;
        G.moveto (s * i) 68 ;
        G.draw_string (Printf.sprintf "[%i] %ix" i worst)
      end
    done

  let draw_sector t i sector =
    let w, _ = t.size in
    let nb_horz = (w - border) / size' in
    let x, y = i mod nb_horz, i / nb_horz in
    let x, y = border + (x * size'), border_bottom + (y * size') in
    let img = t.bitmaps.(i) in
    for y = 0 to size - 1 do
      for x = 0 to size - 1 do
        let x, y = x / factor, y / factor in
        let j = (y * true_size) + x in
        let g =
          try Cstruct.get_uint8 sector j with
          | Invalid_argument _ -> 0
        in
        img.(y).(x) <- colors.(g)
      done
    done ;
    draw_image img x y ;
    G.moveto x y ;
    G.draw_string (string_of_int i) ;
    t.write_count.(i) <- t.write_count.(i) + 1

  let write t id lst =
    let id' = Int64.to_int id in
    List.iteri (fun i s -> draw_sector t (id' + i) s) lst ;
    B.write t.block id lst

  let draw_dead_sector t i =
    let w, _ = t.size in
    let nb_horz = (w - border) / size' in
    let i = Int32.to_int i in
    let x, y = i mod nb_horz, i / nb_horz in
    let x, y = border + (x * size'), border_bottom + (y * size') in
    let arr = t.bitmaps.(i) in
    for y = 0 to Array.length arr - 1 do
      for x = 0 to Array.length arr.(y) - 1 do
        let c = arr.(y).(x) in
        let r, g, b = c lsr 16, (c lsr 8) land 0xFF, c land 0xFF in
        let m = (r + g + b) / 3 in
        let m = m land 0xFF in
        arr.(y).(x) <- G.rgb m m m
      done
    done ;
    draw_image arr x y ;
    (*
       G.set_color G.black ;
       G.set_line_width 4 ;
       G.draw_rect (x + 2) (y + 2) (size - 4) (size - 4) ;
       G.moveto (x + 2) (y + 2) ;
       G.lineto (x + size - 4) (y + size - 4) ;
       G.moveto (x + 2) (y + size - 4) ;
       G.lineto (x + size - 4) (y + 2) ;
       G.set_line_width 1 ;
    *)
    ()

  let discard t i = draw_dead_sector t (Int64.to_int32 i)

  let () =
    G.open_graph " " ;
    G.auto_synchronize false ;
    G.set_color G.white ;
    G.clear_graph () ;
    at_exit (fun () -> G.close_graph ())

  let of_block block =
    let open Lwt.Syntax in
    let+ info = B.get_info block in
    let w, h = G.size_x (), G.size_y () in
    let nb_pages = Int64.to_int info.size_sectors in
    let bitmaps = Array.init nb_pages (fun _ -> Array.make_matrix size size 0x555555) in
    let write_count = Array.make nb_pages 0 in
    let read_count = Array.make nb_pages 0 in
    let t = { block; size = w, h; nb_pages; bitmaps; write_count; read_count } in
    for i = 0 to nb_pages - 1 do
      draw_dead_sector t (Int32.of_int i)
    done ;
    t

  let disconnect t = B.disconnect t.block
  let sleep = ref 0.

  let pause = ref false

  let do_pause () =
    let exception Resume in
    try
      while true do
        match Graphics.read_key () with
        | ' ' | 'p' ->
          Fmt.pr "Graphics: resume execution@." ;
          raise Resume
        | _ -> ()
      done
    with
    | Resume -> ()

  let flush t =
    refresh t ;
    while Graphics.key_pressed () do
      match Graphics.read_key () with
      | ' '
      | 'p' ->
        Fmt.pr "Graphics: pause execution@.";
        do_pause ()
      | _ -> ()
    done;
    if !pause then do_pause ();
    let w, _ = t.size in
    G.set_color G.white ;
    G.fill_rect 0 0 w (border_bottom - 15) ;
    refresh_write_count t ;
    refresh_read_count t ;
    G.synchronize () ;
    Unix.sleepf !sleep ;
    ()
end
