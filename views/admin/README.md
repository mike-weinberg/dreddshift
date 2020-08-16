# admin views
## _wait but isn't **ALL** of this admin stuff?_

Yes. It is. This is a catch all. it's misc_admin, but that's a needlessly long name, and you probably already have an `admin` schema set up -- might as well have one giant schema of admin views than two, right? Well, that's my thinking anyway. 

These views (and maybe also some tables, we'll see) are intended to help you assess the current and past state of your cluster. 

## Why aren't you using DBT?

Maybe later? Submit a PR? I like DBT, but this project doesn't really need it. It won't change very frequently, and you won't need self-managing dependencies. for now DBT can K.I.S.S. my shiny metal ... if you like futurama, you know the rest. 
