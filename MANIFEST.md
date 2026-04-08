# File	            Purpose


1.  tracker_config.json                     Port, SSL, auth, schedule — edit this on each server
2.  app.py                                  Flask app + scraper + scheduler
3.  gunicorn_conf.py	                    Reads config, sets up gunicorn
4.  hash_password.py                        Generate a PBKDF2 password hash for tracker_config.json
5.  deploy/short-interest-tracker.service	Systemd unit file
6.  deploy/*.conf *fail*                    Example vhost proxy, fail2ban configs
7.  templates/index.html	                Frontend
8.  static/css/style.css	                Styles
9.  data/	                                SQLite DB, logs, auto-managed
10. static/charts/                          Saved chart screenshots, auto-pruned
11. watchlist.json                          configurable list of ticker names to poll



