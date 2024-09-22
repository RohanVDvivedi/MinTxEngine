#include<capp.h>

#include<string.h>

void init_temp(temp* t, int s, char* str)
{
	t->s = s;
	strncpy(t->str, str, 29);
	t->str[29] = '\0';
}