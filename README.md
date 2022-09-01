# vac-seen-rollup
Daily count rollup by location code

 oc new-app --name=vac-seen-rollup dotnet:6.0~https://github.com/donschenck/vac-seen-rollup --strategy docker -e MARTEN_CONNECTION_STRING="Host=postgresql;Username=postgres;Password=7f986df431344327b52471df0142e520;" -e MYSQL_CONNECTION_STRING="Server=mariadb;User ID=root;Password=admin;Database=vaxdb;"