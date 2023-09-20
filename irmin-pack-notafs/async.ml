type outcome =
  [ `Success
  | `Cancelled
  | `Failure of string
  ]
[@@deriving irmin]

type status =
  [ `Success
  | `Cancelled
  | `Failure of string
  | `Running
  ]
[@@deriving irmin]

type t = outcome

let async fn : t =
  try
    fn () ;
    `Success
  with
  | err -> `Failure (Printexc.to_string err)

let await (#t as t) = t

let status (#t as t) = t

let cancel (_ : t) = false
