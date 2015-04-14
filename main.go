package main

import (
	"net/http"

	"code.google.com/p/gorest"
)

func main() {
	gorest.RegisterService(new(TransitService))
	http.Handle("/", gorest.Handle())
	http.ListenAndServe(":8787", nil)
}

type TransitService struct {
	gorest.RestService `root:"/tutorial/"`
	helloWorld         gorest.EndPoint `method:"GET" path:"/hello-world/" output:"string"`
	sayHello           gorest.EndPoint `method:"GET" path:"/hello/{name:string}" output:"string"`
}

func (serv TransitService) HelloWorld() string {
	return "Hello World"
}

func (serv TransitService) SayHello(name string) string {
	return "Hello " + name
}
