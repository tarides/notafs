(lang dune 3.1)

(generate_opam_files true)

(name notafs)
(source (github tarides/notafs))
(license ISC)
(authors "Arthur Wendling" "Gwenaëlle Lecat" "Charlène Gros")
(maintainers "arthur@tarides.com")
(version 0.1)

(package
  (name notafs)
  (synopsis "Not a filesystem for MirageOS block devices")
  (depends
    (ocaml (>= "4.12.0"))
    checkseum
    mirage-block
    (mirage-kv (>= "6.1.1"))
    optint
    repr
    ppx_repr
    (mirage-block-unix :with-test)
    (tar-mirage :with-test)
    (graphics :with-test)
    (color :with-test)))

(package
  (name notafs-cli)
  (synopsis "Notafs command-line tool")
  (depends
    (notafs :=version)
    cmdliner
    fmt))

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
    (notafs :=version)
    lwt_direct))

(package
  (name irmin-pack-notafs)
  (synopsis "Irmin-pack using notafs")
  (depends
    (irmin-pack (= "dev"))
    (index-notafs :=version)
    ppx_irmin
    lwt_direct))
