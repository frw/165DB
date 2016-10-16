#include <stdio.h>
#include <stdlib.h>

typedef long valType;
typedef struct _arrayType
{
    valType* data;
    size_t NoRows;
    size_t NoCols;
}arrayType;


void set_value(arrayType* a, int Row, int Col, valType value, int table_type);
valType get_value(arrayType* a, int Row, int Col);
void alloc_array(arrayType *a,int NoRows, int NoCols);

int main (int argc, char** argv)
{
    size_t NoRows;
    size_t NoCols;
    arrayType array;
    int table_type;
    int has_header=1;
    char table_name[100];
    if (argc<5)
    {
        fprintf(stderr,"Please provide 4 arguments:\n\t%s <NoRows> <NoCols> <TableName> <Type> <HasHeader=1,optional>\n",argv[0]);
        fprintf(stderr,"\t\t<Type=0,1,2> 0: all cols sequential integers, 1: last col RAND, 2: last col RAND%50\n");
        fprintf(stderr,"\t\texample: %s 10000000 50 db1.tblBIG 2\n\n",argv[0]);
        return 0;
    }

    NoRows=(size_t)atol(argv[1]);
    NoCols=(size_t)atol(argv[2]);
    sprintf(table_name,"%s",argv[3]);
    table_type=(int)atoi(argv[4]);
    if (argc==6)
        has_header=(int)atoi(argv[5]);

    fprintf(stderr,"Allocating a %ld by %ld array.\n",NoRows,NoCols);
    alloc_array(&array,NoRows,NoCols);

    fprintf (stderr,"Generating data ...\n");
    size_t i,j;
    for (i=0;i<array.NoRows;i++)
    {
        for (j=0;j<array.NoCols;j++)
        {
            //fprintf(stderr,"Setting value for %ld,%ld ... \n",i,j);
            set_value(&array,i,j,(valType)(i+j),table_type);
        }
    }


    fprintf (stderr,"Printing data ...\n");
    if (has_header==1)
    {
        for (j=0;j<(array.NoCols-1);j++)
        {
            printf("%s.col%ld,",table_name,j+1);
        }
        printf("%s.col%ld",table_name,j+1);
        printf("\n");
    }
    for (i=0;i<array.NoRows;i++)
    {
        for (j=0;j<(array.NoCols-1);j++)
        {
           printf("%ld,",get_value(&array,i,j));
        }
        printf("%ld",get_value(&array,i,j));
        printf("\n");
    }

}


void set_value(arrayType* a, int Row, int Col, valType value, int table_type)
{
    if (table_type==0)
	a->data[Row*a->NoCols+Col]=value;
    else if (table_type==1)
    {
	if (Col==(a->NoCols-1))
	{
            srand((unsigned int)value);
            a->data[Row*a->NoCols+Col]=rand();
	}
        else
	    a->data[Row*a->NoCols+Col]=value;
    }
    else if (table_type==2)
    {
	if (Col==(a->NoCols-1))
	{
            srand((unsigned int)value);
            a->data[Row*a->NoCols+Col]=rand()%(a->NoRows/50);
	}
        else
	    a->data[Row*a->NoCols+Col]=value;
    }
}


valType get_value(arrayType* a, int Row, int Col)
{
    return a->data[Row*a->NoCols+Col];
}

void alloc_array(arrayType *a,int NoRows, int NoCols)
{
    a->NoRows=NoRows;
    a->NoCols=NoCols;
    a->data=malloc(sizeof(valType)*a->NoRows*a->NoCols);
}


