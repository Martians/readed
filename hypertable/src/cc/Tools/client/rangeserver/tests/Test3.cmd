UPDATE 'test/Test3' "Test2-data.txt";
pause 1;
CREATE SCANNER ON 'test/Test3'[..??] LIMIT 15;
DESTROY SCANNER;
CREATE SCANNER ON 'test/Test3'[..??] MAX_VERSIONS=2 LIMIT 15;
DESTROY SCANNER;
quit