until $(curl -X POST --output /dev/null --silent --head --fail http://localhost:8000/api/v1/workspaces/list); do
  printf '.'
  sleep 5
done
