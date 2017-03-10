library(mrsdeploy)

service_name <- "Test1"

print("Remote login on R Server...")
remoteLogin("http://52.166.190.135:12800",
        username = "admin",
        password = "Hyperloop2017!",
        session = FALSE)
print("Done remote login on R Server.")
services <- listServices(service_name)
serv <- services[[1]]
api <- getService(serv$name, serv$version)
print(sprintf("Service name: %s Service version: %s",serv$name, serv$version))

print("Preparing local data...")
test_df <- data.frame(x = c(1, 1, 1, 1),
                      y = c(2, 2, 2, 2),
                      z = c(1, 1, 1, 1))
print("Dataframe:")
print(test_df)
print("Done preparing local data.")

print("Webservice execution on R Server...")
resp <- api$test_func(df = test_df)
res <- resp$output("answer")
print(sprintf("Result from server: %.2f",res))

remoteLogout()
