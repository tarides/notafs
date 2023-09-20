(lang dune 3.1)

(generate_opam_files true)

(name notafs)
(source (github art-w/notafs))
(license ISC)
(authors "Arthur Wendling")
(maintainers "art.wendling@gmail.com")
(version 0.1)

(package
  (name notafs)
  (synopsis "Not a filesystem for MirageOS block devices")
  (depends
    (ocaml (>= "4.12.0"))
    mirage-block
    repr
    ppx_repr))

(package
  (name lwt_direct)
  (synopsis "Direct style for Lwt")
  (depends
    (ocaml (>= "5.0.0"))
    lwt))

(package
  (name index-notafs)
  (synopsis "Index using notafs")
  (depends
    (index (= "dev"))
    notafs
    lwt_direct))

(package
  (name irmin-pack-notafs)
  (synopsis "Irmin-pack using notafs")
  (depends
    (irmin-pack (= "dev"))
    index-notafs
    ppx_irmin
    lwt_direct))
