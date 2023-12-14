module Make (B : Context.A_DISK) = struct
  module Sector = Sector.Make (B)
  module Schema = Schema.Make (B)

  type t = Sector.t

  open Lwt_result.Syntax

  let get t i =
    let pos = i / 8 in
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = value land (1 lsl offset) in
    Lwt_result.return (flag = 0)

  let free t i =
    let pos = i / 8 in
    if true then Format.printf "Sector %d being freed@." i ;
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = value land (1 lsl offset) in
    (* Format.printf "Value %d Flag %d@." value flag; *)
    assert (flag > 0) ;
    let update = value lxor (1 lsl offset) in
    if pos = 0 then Format.printf "Value %d; Update %d; Flag %d@." value update flag ;
    Sector.set_uint8 t pos update

  let use t i =
    let pos = i / 8 in
    if true then Format.printf "Sector %d being used@." i ;
    let* value = Sector.get_uint8 t pos in
    let offset = i mod 8 in
    let flag = value land (1 lsl offset) in
    assert (flag = 0) ;
    let update = value lor (1 lsl offset) in
    if pos = 0 then Format.printf "Value %d; Update %d; Flag %d@." value update flag ;
    (* Format.printf "Value %d Flag %d Offset %d Update %d@." value flag offset update; *)
    Sector.set_uint8 t pos update

  let create () =
    Format.printf "create ;) @." ;
    let* t = Sector.create () in
    let sz = B.page_size in
    let rec init = function
      | i when i >= sz -> Lwt_result.return ()
      | i ->
        let* () = Sector.set_uint8 t i 0 in
        init (i + 1)
    in
    let rec init_reserved = function
      | i when i < 0 -> Lwt_result.return ()
      | i ->
        let* () = use t i in
        init_reserved (i - 1)
    in
    let* () = init 0 in
    let+ () = init_reserved 12 in
    t
end
