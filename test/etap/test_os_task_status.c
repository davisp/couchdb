// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define ID_LEN 32
char taskid[ID_LEN+1];

int
read_taskid()
{
    char c = '\0';
    size_t num = 1;
    size_t pos = 0;
    
    while(c != '\"' && num > 0) {
        num = fread(&c, 1, 1, stdin);
    }
    
    if(num <= 0)
        return 0;
    
    num = fread(&c, 1, 1, stdin);
    while(c != '\"' && pos < ID_LEN && num > 0) {
        taskid[pos++] = c;
        num = fread(&c, 1, 1, stdin);
    }
    
    if(num <= 0 || pos != ID_LEN)
        return 0;
    
    taskid[ID_LEN] = '\0';
    
    while(c != '\n' && num > 0) {
        num = fread(&c, 1, 1, stdin);
    }
    
    if(num <= 0 || c != '\n')
        return 0;
    
    return 1;
}

int
main(int argc, const char * argv[])
{
    char c = '\0';
    size_t num = 1;
    
    fprintf(stdout, "[\"task_create\", \"task\", \"type\", \"status\"]\n");
    fflush(stdout);

    if(!read_taskid()) {
        fprintf(stderr, "Failed to parse task id.\n");
        exit(1);
    }

    sleep(1);
    
    fprintf(stdout, "[\"task_update\", \"%s\", \"new status\"]\n", taskid);
    fflush(stdout);
    
    sleep(1);
    
    fprintf(stdout, "[\"task_done\", \"%s\"]\n", taskid);
    fflush(stdout);

    sleep(20);    
    exit(0);
}
