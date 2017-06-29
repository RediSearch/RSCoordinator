%token_type {Token}

%include {
	#include <stdlib.h>
	#include <stdio.h>
	#include <assert.h>
	#include "token.h"	
	#include "grammar.h"
    #include "parser_ctx.h"
    #include "../cluster.h"
    #include "../node.h"
    #include "../endpoint.h"

    #include "lexer.h"
	void yyerror(char *s);


} // END %include

%syntax_error {  
    asprintf(&ctx->errorMsg, "Syntax error at offset %d near '%.*s'\n", TOKEN.pos,(int)TOKEN.len, TOKEN.s);
    ctx->ok = 0;
}  
  
%default_type { char * }
%default_destructor {  printf("freeing %p\n", $$); free($$); }
%extra_argument { parseCtx *ctx }
%type shard { RLShard }
%destructor shard {
	MRClusterNode_Free(&$$.node);
}
%type endpoint { MREndpoint }
%destructor endpoint { MREndpoint_Free(&$$); }

%type topology { MRClusterTopology *}
%destructor topology { MRClusterTopology_Free($$); }

%type master {int}
%destructor master {} 
%type has_replication {int}
%destructor has_replication {} 


root ::= MYID shardid(B) has_replication(C) topology(D). {
    ctx->my_id = B;
    ctx->replication = C;
    ctx->topology = D;
	// detect my id and mark the flag here
    for (size_t s = 0; s < ctx->topology->numShards; s++) {
        for (size_t n = 0; n < ctx->topology->shards[s].numNodes; n++) {
            if (!strcmp(ctx->topology->shards[s].nodes[n].id, ctx->my_id)) {
                printf("My Node: %s!\n", ctx->my_id);
                ctx->topology->shards[s].nodes[n].flags |= MRNode_Self;
            }
        }
    }
}

topology(A) ::= RANGES INTEGER(B) . {
    
    A = MR_NewTopology(B.intval, 4096);
}
//topology -> shardlist -> shard -> endpoint

topology(A) ::= topology(B) shard(C). {
    MRTopology_AddRLShard(B, &C);
    A = B;
}

has_replication(A) ::= HASREPLICATION . {
    A =  1;
}

has_replication(A) ::= . {
    A =  0;
}

shard(A) ::= SHARD shardid(B) SLOTRANGE INTEGER(C) INTEGER(D) endpoint(E) master(F). {
	
	A = (RLShard){
			.node = (MRClusterNode) {
			.id = B,
			.flags = MRNode_Coordinator | (F ? MRNode_Master : 0),
			.endpoint = E,
		},
		.startSlot = C.intval,
		.endSlot = D.intval,
	};
}


shardid(A) ::= STRING(B). {
	A = B.strval;
}
shardid(A) ::= INTEGER(B). {
	asprintf(&A, "%lld", B.intval);
}

endpoint(A) ::= tcp_addr(B). {
	MREndpoint_Parse(B, &A);
    free(B);
}

endpoint(A) ::= endpoint(B) unix_addr(C) . {
  	B.unixSock = C; 
	A = B;
}


tcp_addr(A) ::= ADDR STRING(B) . {
    A = B.strval;
} 

unix_addr(A) ::= UNIXADDR STRING(B). {
	A = B.strval;
}

master(A) ::= MASTER . {
    A = 1;
}

master(A) ::= . {
    A = 0;
}

%code {


MRClusterTopology *MR_ParseTopologyRequest(const char *c, size_t len, char **err)  {

    //printf("Parsing query %s\n", c);
    yy_scan_bytes(c, len);
    void* pParser =  MRTopologyRequest_ParseAlloc (malloc);        
    int t = 0;

    parseCtx ctx = {.topology = NULL, .ok = 1, .replication = 0, .errorMsg = NULL };
    //ParseNode *ret = NULL;
    //ParserFree(pParser);
    while (ctx.ok && 0 != (t = yylex())) {
        MRTopologyRequest_Parse(pParser, t, tok, &ctx);                
    }
    if (ctx.ok) {
        MRTopologyRequest_Parse (pParser, 0, tok, &ctx);
    }
    
    MRTopologyRequest_ParseFree(pParser, free);

    if (err) {
        *err = ctx.errorMsg;
    }
    return ctx.topology;
  }


   
}

