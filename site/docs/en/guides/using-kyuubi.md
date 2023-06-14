
Terminal supports interfacing with Kyuubi to submit SQL to Kyuubi for execution. All you need to do is add the Kyuubi configuration as instructed below:
```shell
ams:
  arctic.ams.terminal.backend: kyuubi
  arctic.ams.terminal.kyuubi.jdbc.url: jdbc:hive2://127.0.0.1:10009/  # kyuubi Connection Address
```
Without configuring Kyuubi, Terminal executes in memory in AMS.

To execute SQL in Terminal, you can refer to the following steps:：

- Please switch Catalog first
- Before writing SQL, you can use the provided SQL Shortcuts function to help you build SQL quickly.
- Writing SQL
- Click the Execute button to run the SQL;

![terminal](../images/admin/terminal_introduce.png)
