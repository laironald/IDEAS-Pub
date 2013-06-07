IDEAS RESTful API
=================

### Vanilla Installation Guide

As of this writing. I develop my software on Ubuntu 12.10. It likely does not matter what platform you use as the goal of the software is to support as many flavors as Ubuntu as possible. The following steps below have been tested on Ubuntu 12.04, 12.10 and 13.04.

The following is my "stack":

  * Ubuntu 12.10
  * MySQL 5.5
  * Python 2.7
  * Flask 0.9

I am making some basic assumptions:

  * The reader of the guide is familar with operating a terminal within Ubuntu.
  * Memory and space requirements depends on the complexity of the queries and the size of the underlying data. The user will determine this.
  * The installation should be smooth requiring less than a total of 20 minutes.


### Getting started

To get started, you'll need to have a few tools installed on your server. From this point forward, we will be using our Terminal to begin our installation.

    sudo apt-get install git
    sudo apt-get install python-pip
    sudo apt-get install python-mysqldb


### Installing MySQL

For this project, I use MySQL. I realize there are other databases and a later goal is to support database ORMs such as [SQLAlchemy](http://www.sqlalchemy.org/) to allow easy support of these. If you have a MySQL server on a separate server, a cloud service, etc -- feel free to use that instead. 

However, if you would like to install the RESTful API on a single server:

    sudo apt-get install mysql-server

Follow the steps of the installation. For my example, I use the password "toor". When you see "toor", please realize that this is the default password. Your root password is recommended to be different.

Create a database for MySQL which will store the API data. First log into MySQL using your credentials. For those new to MySQL, -u indicates username and -p indicates password.

    mysql -u root -ptoor

Once you've logged into MySQL, we must create a database where we would like to store the API data. The example below shows how to create a database called "goideas". This matches the file in the [IDEAS-Pub configuration file](https://github.com/laironald/IDEAS-Pub/blob/master/IDEAS/config/__init__.py) (which will be described further below).

    create database goideas;
    exit


### Clone the GitHub repository

In a directory that you find convenient. (I use my default user directory), clone the repository. For the time being, I use GitHub.

    git clone https://github.com/laironald/IDEAS-Pub.git

There are several directories, many of which are libraries to support the RESTful interface. The one that needs to be configured is the config directory: [~/IDEAS-Pub/IDEAS/config/__init__.py](https://github.com/laironald/IDEAS-Pub/blob/master/IDEAS/config/__init__.py)

On your local machine, configure this file so it matches with the settings to connect to MySQL. For the time being, the account requires both read and write access to the database. I realize this may be sensitive for security purposes and is an item to address in future updates to the software. I do not recommend using your root username and password, even though this is how it is currently setup.


### Installing the Python Library

Installing the IDEAS RESTful API is more or less two steps:

    sudo pip install -r requirements.txt
    sudo python setup.py install

Note that if you decide to modify the source code or the configuration files, you must re-install the application. You may also need to restart Apache2. Details on installing Apache2 is below. Restarting Apache2 is as simple as:

    sudo service apache2 restart


### Configuring Flask for Apache2

There is a more thorough guide about installing Flask with Apache2. It's probably not necessary, but feel free to read about it within the [Flask documentation](http://flask.pocoo.org/docs/deploying/mod_wsgi/).

The first step is to install the Apache HTTP Server

    sudo apt-get install apache2
    sudo apt-get install libapache2-mod-wsgi

Then, modify the Apache default configuration file. The file on Ubuntu is located on Ubuntu at: /etc/apache2/sites-available/default. To modify this document, you will need to use sudo. Include the configuration options between the two elipsis: (...)

    <VirtualHost *:80>
        ServerAdmin webmaster@localhost

        ...

        WSGIDaemonProcess ideas user=www-data group=www-data threads=5
        WSGIScriptAlias /v1 /var/www/ideasapi/start.wsgi

        <Directory /var/www/ideasapi>
            WSGIProcessGroup ideas
            WSGIApplicationGroup %{GLOBAL}
            Order deny,allow
            Allow from all
        </Directory>

        ...

    </VirtualHost>

You will find a sub directory called ideasapi which is required to set up Apache2 for a production environment. [~/IDEAS-Pub/ideasapi](https://github.com/laironald/IDEAS-Pub/tree/ideasapi). Clone this folder to /var/www.  For the time being, please follow the directions above, but certainly understandable if there are other preferences for the structure

    sudo cp -r ~/IDEAS-Pub/ideasapi /var/www/

To conclude, you must restart your server.

    sudo service apache2 restart

For reference, log files for Apache2 are stored in the following directory: /var/apache2/log. This can be helpful to observe if there are errors related to interacting with web service.


### Loading data

For convenience, we have generated sample API data. (thanks Sonya). The sample data is located [~/IDEAS-Pub/sample](https://github.com/laironald/IDEAS-Pub/tree/master/sample) 

There are multiple ways to import data into the API. The first involves the file, [output.sql.tar.gz](https://github.com/laironald/IDEAS-Pub/blob/master/sample/output.sql.tar.gz) where as the second approach involves the csv files and [ingest.py](https://github.com/laironald/IDEAS-Pub/blob/master/sample/ingest.py)

##### Approach 1: MySQL
First untar the tar file to generate a output.sql file. 

    tar -xzf output.sql.tar.gz

A output.sql file is generated which is a plaintext file which can be loaded into MySQL. To do this, login to your MySQL server. The goideas provided below is the database that we created in a step above. As noted: if your MySQL server exists on another host, please login that host.

    mysql -u root -ptoor goideas

Once you have logged into your MySQL server, simply load the data by using the source command. You must be accessing this in the same directory the file is located in, otherwise you will have to change the second portion of the command.

    source output.sql
    exit

##### Approach 2: CSV files and ingest.py

This method is a bit more involved, but it is not meant to be complicated. In the directory, you will find a file called [ingest.py](https://github.com/laironald/IDEAS-Pub/blob/master/sample/ingest.py) and [schema.csv](https://github.com/laironald/IDEAS-Pub/blob/master/sample/schema.csv). These two files are used to build the MySQL databases and contain the same information as the output.sql.tar.gz in Approach 1.

schema.csv appears as the following and is purely optional

    ...

    POP_publication_object,variable,title,
    POP_publication_object,type,text,

    ...

This translates to for the POP_publication_object file, the variable name "title" should actually be "text". The ingest script does its best in guessing the most appropriate data type, but can be wrong. Using this method overrides the default behavior. The data types can be found by understanding the MySQL data types (http://dev.mysql.com/doc/refman/5.5/en/data-types.html)

Once the schema file is setup, running the ingest.py script is the final step. There are three options provided.

 1. ingest -- ingest all csv files in the current directory. This means if there are files that do not need to be reloaded, they do not need to be in this directory.
 2. backup -- create a output.sql.tar.gz file to transport to others. this may be a quicker means to load data as in Approach 1.
 3. drop -- this eliminates the data. This may be useful because there is a cache that is generated. Dropping the data allows us to clear the cache and all the data. 

A natural data flow for updating the data would be the following.

 1. ingest the data
 2. if necessary to clear the cache, backup the data
 3. if necessary to clear the cache, drop the data
 4. using Approach 1, re-load the data

The command to ingest the files is as follows

    python ingest.py ingest




### Final stuff

Moving forward, updating the software is easy. When the software gets updated on GitHub, simply enter the directory (~/IDEAS-Pub) and pull. Re-install the python files and restart apache2. There is no dedicated timing on when the software will be updated.

    git pull
    pip install -r requirements.txt
    sudo python setup.py install
    sudo service apache2 restart

If you make some awesome modifications to the software, please let me know. Fork it. Collaborate with me. Etc. For now, I have my own private repository of this software and this directory is merely a more polished clone. My goal is to stabalize a few more items so I can feel confident about making it a truly collaborative and open system!


### Relevant links

  * [Apache2](http://httpd.apache.org/)
  * [GitHub](https://github.com/)
  * [Python Flask](http://flask.pocoo.org/)
  * [MySQL](http://www.mysql.com/)
  * [Ubuntu](http://www.ubuntu.com/)
