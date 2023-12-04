module type S = sig
  type t

  val compare : t -> t -> int
  val to_string : t -> string
  val add : t -> int -> t
end

module Make (Id : S) = struct
  module Diet = Map.Make (Id)

  type t = int Diet.t

  let empty = Diet.empty

  let add_range t (k, len) =
    let mlo =
      match Diet.find_last (fun key -> Id.compare key (Id.add k (len - 1)) <= 0) t with
      | mlk, mlv -> begin
        match Id.compare k (Id.add mlk mlv) with
        | 0 -> Some mlv
        | r when r > 0 -> None
        | _ -> assert false
      end
      | exception Not_found -> None
    in
    let mro = Diet.find_opt (Id.add k len) t in
    match mlo, mro with
    | Some mlv, Some mrv ->
      let t = Diet.remove (Id.add k len) t in
      Diet.add (Id.add k (-mlv)) (mlv + len + mrv) t
    | Some mlv, None -> Diet.add (Id.add k (-mlv)) (mlv + len) t
    | None, Some mrv ->
      let t = Diet.remove (Id.add k len) t in
      Diet.add k (mrv + len) t
    | None, None -> Diet.add k len t

  let add t k = add_range t (k, 1)

  let to_list t =
    Diet.fold
      (fun s l acc ->
        let rec explode v n lst =
          if n = 0 then lst else explode (Id.add v 1) (n - 1) (v :: lst)
        in
        explode s l acc)
      t
      []

  let of_list l =
    let t = empty in
    let rec adding t l =
      match l with
      | [] -> t
      | hd :: tl -> adding (add t hd) tl
    in
    adding t l

  let to_range_list t = Diet.bindings t
end
