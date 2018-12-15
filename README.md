# satomic

[![Build Status](https://img.shields.io/travis/dhui/satomic/master.svg)](https://travis-ci.org/dhui/satomic) [![Code Coverage](https://img.shields.io/codecov/c/github/dhui/satomic.svg)](https://codecov.io/gh/dhui/satomic) [![GoDoc](https://godoc.org/github.com/dhui/satomic?status.svg)](https://godoc.org/github.com/dhui/satomic) [![Go Report Card](https://goreportcard.com/badge/github.com/dhui/satomic)](https://goreportcard.com/report/github.com/dhui/satomic) [![GitHub Release](https://img.shields.io/github/release/dhui/satomic/all.svg)](https://github.com/dhui/satomic/releases) ![Supported Go versions](https://img.shields.io/badge/Go-1.11-lightgrey.svg)

satomic is a Golang package that makes managing nested SQL transactions/savepoints easier

## What's with the name?
Go **S**QL **atomic** => satomic

---

Inspired by [sqlexp](https://github.com/golang-sql/sqlexp) and Django's [atomic](https://docs.djangoproject.com/en/2.1/topics/db/transactions/#django.db.transaction.atomic) decorator/context manager.