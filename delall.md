docker stop $(docker ps -aq)

docker system prune -a --volumes -f

docker volume rm $(docker volume ls -q)
