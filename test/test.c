#include<capp.h>
#include<print_temp.h>

int main()
{
	temp t = {.s = 7, .str = "Hello World"};
	print_temp_test(t);
	return 0;
}