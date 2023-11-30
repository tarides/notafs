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

  let add t k =
    let mlo =
      match Diet.find_last (fun key -> Id.compare key k <= 0) t with
      | mlk, mlv ->
        assert (Id.compare k (Id.add mlk mlv) >= 0) ;
        if Id.compare k (Id.add mlk mlv) = 0 then Some mlv else None
      | exception Not_found -> None
    in
    let mro = Diet.find_opt (Id.add k 1) t in
    match mlo, mro with
    | Some mlv, Some mrv ->
      let t = Diet.remove (Id.add k 1) t in
      Diet.add (Id.add k (-mlv)) (mlv + mrv + 1) t
    | Some mlv, None -> Diet.add (Id.add k (-mlv)) (mlv + 1) t
    | None, Some mrv ->
      let t = Diet.remove (Id.add k 1) t in
      Diet.add k (mrv + 1) t
    | None, None -> Diet.add k 1 t

  let to_list t =
    Diet.fold
      (fun s l acc ->
        let rec explode v n lst =
          if n = 0 then lst else explode (Id.add v 1) (n - 1) (v :: lst)
        in
        explode s l acc)
      t
      []
end
