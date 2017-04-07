%token_type {Token}

%include {
	#include <stdlib.h>
	#include <stdio.h>
	#include "token.h"	
	#include "grammar.h"
    #include "parser_ctx.h"
    #include "../dep/rmr/cluster.h"
    #include "../dep/rmr/node.h"
    #include "../dep/rmr/endpoint.h"

    
	void yyerror(char *s);
} // END %include

%extra_argument { parseCtx *ctx }
%type shard { RLShard }
%type endpoint { MREndpoint }

%type shardid { char * }
%type tcp_addr { char * }
%type unix_addr { char * }
%type master {int}
%type has_replication {int}



root ::= MYID STRING(B) has_replication(C) topology(D). {
    ctx->my_id = B.strval;
    ctx->replication = C;
    ctx->topology = D;
	// TODO: detect my id and mark the flag here
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
	asprintf(&A, "%d", B.intval);
}

endpoint(A) ::= tcp_addr(B). {
	MREndpoint_Parse(B, &A);
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

	/* Definitions of flex stuff */
	typedef struct yy_buffer_state *YY_BUFFER_STATE;
	int             yylex( void );
	YY_BUFFER_STATE yy_scan_string( const char * );
  	YY_BUFFER_STATE yy_scan_bytes( const char *, size_t );
  	extern int yylineno;
  	extern char *yytext;
	extern int yycolumn;

	QueryExpressionNode *Query_Parse(const char *q, size_t len, char **err) {
		yycolumn = 1;	// Reset lexer's token tracking position
		yy_scan_bytes(q, len);
  		void* pParser = ParseAlloc(malloc);
  		int t = 0;

		parseCtx ctx = {.root = NULL, .ok = 1, .errorMsg = NULL};

  		while( (t = yylex()) != 0) {
			Parse(pParser, t, tok, &ctx);
		}
		if (ctx.ok) {
			Parse(pParser, 0, tok, &ctx);
  		}
		ParseFree(pParser, free);
		if (err) {
			*err = ctx.errorMsg;
		}
		return ctx.root;
	}
}