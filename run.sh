docker build -t mopidy-sound-visualizer-backend ./ &&
docker run -p 3000:3000 -p 5556:5556/udp --env GIT_SHA="$(git rev-parse HEAD)" mopidy-sound-visualizer-backend
