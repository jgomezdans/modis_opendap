This repository contains a tool to download data from the all new MODIS OpenDAP service. Note that this tool only really deals with daily reflectance, but can be easily extended to other MODIS (or ASTER) products that are made available from this server. Since the server has just been opened, expect things not to work. 

We use multiple threads to download in parallel. Note that if you abuse this, you might overwhelm the server and it might well kick you out or something. Beware of this...

Also, you need the pydap library install. You can do this easily with pip:

    pip install pydap

I think I use 3.1.1 which appears to be the latest...

If you have any questions, email the author on j.gomez-dans@ucl.ac.uk


