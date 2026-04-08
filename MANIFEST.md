## Manifest



|  File / Dir                            | Purpose                                                  |
|:---------------------------------------|:---------------------------------------------------------|
|  tracker_config.json                   |  Port, SSL, auth, schedule — edit this on each server    |
|  app.py                                |  Flask app + scraper + scheduler                         |
|  gunicorn_conf.py	                     |  Reads config, sets up gunicorn                          |
|  hash_password.py                      |  Generate a PBKDF2 password hash for tracker_config.json |
|  deploy/short-interest-tracker.service |  Systemd unit file                                       |
|  deploy/*.conf *fail*                  |  Example vhost proxy, fail2ban configs                   |
|  templates/index.html	                 |  Frontend                                                |
|  static/css/style.css	                 |  Styles                                                  |
|  data/	                             |  SQLite DB, logs, auto-managed                           |
|  static/charts/                        |  Saved chart screenshots, auto-pruned                    |
|  watchlist.json                        |  configurable list of ticker names to poll               |



