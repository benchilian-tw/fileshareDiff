### fileshareDiff

Use python asyncio magic to compare source and destination Azure Fileshare for incremental migrations.

### Prerequisite

Python >= 3.8

#### Under the hood

In order to utilize the CPU resources. We use **aiomultiprocess** and **asyncio** to boost the performance as much as possible. 

### Purpose

Assume that you need to migrate one Azure Fileshare to another Azure Fileshare.  and the Azure Fileshare has TB(PB)-Level data size and tons of small files.   

You can use tools like azcopy and robocopy to migrate the data.  but you wanna know the difference between source and destination Fileshare for business purposes?  How can you achieve this?

You can try this script,  this script will compare two different Azure Fileshares and generate a report about the difference. like how many files need to add, how many files need to delete, etc... 

### Install

```bash
[root@localhost]:~# pip3 install -r requirement.txt
```

### Usage

Running the diff of fileshare is easy :

```bash
# export source fileshare environment
[root@localhost]:~# export src_account_name="account_name"
[root@localhost]:~# export src_access_key="key"
[root@localhost]:~# export src_fileshare="fileshare_name"

# export destination fileshare environment
[root@localhost]:~# export dest_account_name="dest_account_name"
[root@localhost]:~# export dest_access_key="key"
[root@localhost]:~# export dest_fileshare="fileshare_name"

# run the scripts
[root@localhost]:~# python3 main.py
fetch from source consumes 35.48441553115845s
fetch from dest consumes 35.761884927749634s
incremental files nums needed to add:2
incremental files nums needed to delete:100
{'/test/1.txt', '/test/2.txt'} {'/23416710-9c3a-4be6-86fe-24295f997df7/c3379542-c52d-43cc-aa6f-4877d4b1cea8',.....}
```

### Reference

You can write your own scripts by referring to the docs below:

https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#shared-key-format-for-2009-09-19-and-later

https://github.com/omnilib/aiomultiprocess