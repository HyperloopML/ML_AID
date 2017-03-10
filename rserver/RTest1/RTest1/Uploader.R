    
library(mrsdeploy)

print("Creating code...")
test_func <- function(df) {
    s <- sum(df)
    return(s)
}
print("Done creating code.")

print("Remote login on R Server...")
remoteLogin("http://52.166.190.135:12800",
        username = "admin",
        password = "Hyperloop2017!",
        session = FALSE)
print("Done remote login on R Server.")

print("Publishing web service ...")
api <- publishService(
     "Test1",
     code = test_func,
     #model = ml_Model,
     inputs = list(df = "data.frame"),
     outputs = list(answer = "numeric"),
     v = "v0.0.1"
)
print("Done publishing web service.")

swagger <- api$swagger()
cat(swagger, file = "swagger2.json", append = FALSE)

remoteLogout()