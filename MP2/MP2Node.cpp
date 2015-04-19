/**********************************
 * FILE NAME: MP2Node.cpp
 * AUTHOR: Chaitanya Datye
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: addQuery
 *
 * DESCRIPTION: This function does the following:
 *              1) Takes the message that was sent to the replica
 *              2) Adds it to the map with key as the transID
 *              3) At most three queries per transID
 */
void MP2Node::addQuery(Message msg) {
    map<int, vector<Message>>::iterator itr;
    itr = this->queries.find(msg.transID);
    itr->second.push_back(msg);
}

/**
 * FUNCTION NAME: addToQuorum
 *
 * DESCRIPTION: This function does the following:
 *              1) Adds a reply message to the quorum or replies
 *              2) Checks if the reply was successful or not
 *              3) If the reply was a success, imcrements the count of successful replies for that transID
 *              4) This qourum count of successes is checked each time
 */
void MP2Node::addToQuorum(Message msg) {
    map<int, pair<int, vector<Message>>>::iterator itr;
    itr = this->quorum.find(msg.transID);
    itr->second.second.push_back(msg);
    if(msg.success)
        itr->second.first++;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;
    vector<Node> prev_hasMyreplicas;
    vector<Node> prev_haveReplicasOf;
    vector<Node> new_hasMyReplicas;
    vector<Node> new_haveReplicasOf;
    int ringsize;
	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
    this->ring = curMemList;
    ringsize = this->ring.size();
    prev_hasMyreplicas = this->hasMyReplicas;
    prev_haveReplicasOf = this->haveReplicasOf;
    
    vector<Node>::iterator itr_n;
    //Calculate the new hasMyReplicas and haveReplicasOf vectors from the new updated ring
    int i=0;
    for(i=0; i<ringsize; i++) {
        if(this->ring.at(i).nodeAddress == this->memberNode->addr){
            if(i==0){
                new_hasMyReplicas.push_back(this->ring.at(i+1));
                new_hasMyReplicas.push_back(this->ring.at(i+2));
                new_haveReplicasOf.push_back(this->ring.at(ringsize-1));
                new_haveReplicasOf.push_back(this->ring.at(ringsize-2));
                break;
            }
            else if(i==1) {
                new_hasMyReplicas.push_back(this->ring.at(i+1));
                new_hasMyReplicas.push_back(this->ring.at(i+2));
                new_haveReplicasOf.push_back(this->ring.at(i-1));
                new_haveReplicasOf.push_back(this->ring.at(ringsize-1));
                break;
            }
            else if(i==ringsize-2){
                new_hasMyReplicas.push_back(this->ring.at(ringsize-1));
                new_hasMyReplicas.push_back(this->ring.at(0));
                new_haveReplicasOf.push_back(this->ring.at(i-1));
                new_haveReplicasOf.push_back(this->ring.at(i-2));
                break;
            }
            else if(i==ringsize-1){
                new_hasMyReplicas.push_back(this->ring.at(0));
                new_hasMyReplicas.push_back(this->ring.at(1));
                new_haveReplicasOf.push_back(this->ring.at(i-1));
                new_haveReplicasOf.push_back(this->ring.at(i-2));
                break;
            }
            else{
                new_hasMyReplicas.push_back(this->ring.at(i+1));
                new_hasMyReplicas.push_back(this->ring.at(i+2));
                new_haveReplicasOf.push_back(this->ring.at(i-1));
                new_haveReplicasOf.push_back(this->ring.at(i-2));
                break;
            }
            
        }
    }
    int prev_size = prev_hasMyreplicas.size();
    int prev_size2 = prev_haveReplicasOf.size();
    
    //Check if there was a change in the previous and new hasMyReplicas and haveReplicasOf vectors
    for(i=0; i<2; i++){
        if(prev_size > 0 && prev_hasMyreplicas[i].nodeAddress.getAddress() != new_hasMyReplicas[i].nodeAddress.getAddress()){
            change = true;
            break;
        }
        if(prev_size2 > 0 && prev_haveReplicasOf[i].nodeAddress.getAddress() != new_haveReplicasOf[i].nodeAddress.getAddress()){
            change = true;
            break;
        }
    }
    
    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    
    //If there was a change call the stabilization protocol to re-organize the keys
    if(change){
       stabilizationProtocol(this->hasMyReplicas, this->haveReplicasOf, new_hasMyReplicas, new_haveReplicasOf);
    }
    
    //Once stabilized, update the current hasMyReplicas and haveReplicasOf vectors for a particular node
    this->hasMyReplicas = new_hasMyReplicas;
    this->haveReplicasOf = new_haveReplicasOf;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    
    vector<Node> replicas = findNodes(key);
    string msgtostring;
    int i=0;
    vector<Message> vlist_quorum;
    vector<Message> vlist_queries;
    pair<int, vector<Message>> p = make_pair(0, vlist_quorum);
    this->quorum.emplace(g_transID, p);
    this->logged.emplace(g_transID, 0);
    this->queries.emplace(g_transID, vlist_queries);
    for(i=0; i<replicas.size(); i++) {
        Message msg(g_transID, this->memberNode->addr, CREATE, key, value, (ReplicaType) i);
        msgtostring = msg.toString();
        addQuery(msg);
        emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, msgtostring);
    }
    g_transID++;
    
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	
    vector<Node> replicas = findNodes(key);
    string msgtostring;
    int i=0;
    vector<Message> vlist_quorum;
    vector<Message> vlist_queries;
    vector<Message> vlist_readreplies;
    pair<int, vector<Message>> p = make_pair(0, vlist_quorum);
    this->quorum.emplace(g_transID, p);
    this->logged.emplace(g_transID, 0);
    this->queries.emplace(g_transID, vlist_queries);
    this->readreplies.emplace(g_transID, vlist_readreplies);
    
    for(i=0; i<replicas.size(); i++) {
        Message msg(g_transID, this->memberNode->addr, READ, key);
        msgtostring = msg.toString();
        addQuery(msg);
        emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, msgtostring);
    }
    g_transID++;
    
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	
    vector<Node> replicas = findNodes(key);
    string msgtostring;
    int i=0;
    vector<Message> vlist_quorum;
    vector<Message> vlist_queries;
    pair<int, vector<Message>> p = make_pair(0, vlist_quorum);
    this->quorum.emplace(g_transID, p);
    this->logged.emplace(g_transID, 0);
    this->queries.emplace(g_transID, vlist_queries);
    for(i=0; i<replicas.size(); i++) {
        Message msg(g_transID, this->memberNode->addr, UPDATE, key, value, (ReplicaType) i);
        msgtostring = msg.toString();
        addQuery(msg);
        emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, msgtostring);
    }
    g_transID++;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	
    vector<Node> replicas = findNodes(key);
    string msgtostring;
    int i=0;
    vector<Message> vlist_quorum;
    vector<Message> vlist_queries;
    pair<int, vector<Message>> p = make_pair(0, vlist_quorum);
    this->quorum.emplace(g_transID, p);
    this->logged.emplace(g_transID, 0);
    this->queries.emplace(g_transID, vlist_queries);
    
    for(i=0; i<replicas.size(); i++) {
        Message msg(g_transID, this->memberNode->addr, DELETE, key);
        msgtostring = msg.toString();
        addQuery(msg);
        emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, msgtostring);
    }
    g_transID++;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
    // Insert key, value, replicaType into the hash table
    Entry entry(value, par->getcurrtime(), replica);
    string hash_entry(entry.convertToString());
    if(this->ht->create(key, hash_entry))
        return true;
    else
        return false;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	
	// Read key from local hash table and return value
    return this->ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	
	// Update key in local hash table and return true or false
    
    Entry entry(value, par->getcurrtime(), replica);
    string hash_entry(entry.convertToString());
    if(this->ht->update(key, hash_entry))
        return true;
    else
        return false;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	
	// Delete the key from the local hash table
    
    if(this->ht->deleteKey(key))
        return true;
    else
        return false;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	
	char * data;
	int size;
    
    int rep_count = 0;          //Count of replies
    int read_rep_count = 0;     //Count of read replies
    MessageType msgtype;
    string msgkey;
    string msgvalue;
    int msgtransID;
    string read_value;
    int flag = 0;
    int logged = 0;
    MessageType querytype;
    string querykey;

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
        bool ack_val;                                   //Tells if a transaction was a success or failure
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
        Message msg(message);
        
        msgtype = msg.type;
        msgkey = msg.key;
        msgtransID = msg.transID;
        
        if(msg.type == CREATE){
            //Use transID = -1 to differentiate between CREATE messages and REPLICATE messages.
            if(msgtransID == -1) {
                //Replication
                createKeyValue(msg.key, msg.value, msg.replica);
            }
            else{
                if(createKeyValue(msg.key, msg.value, msg.replica)) {
                //Log at Server Side
                    this->log->logCreateSuccess(&this->memberNode->addr, false, msg.transID, msg.key, msg.value);
                    ack_val = true;
                }
                else {
                    this->log->logCreateFail(&this->memberNode->addr, false, msg.transID, msg.key, msg.value);
                    ack_val = false;
                }
                Message ack(msg.transID, this->memberNode->addr, REPLY, ack_val);
                string acktostring = ack.toString();
                        
                emulNet->ENsend(&memberNode->addr, &msg.fromAddr, acktostring);
            }
        }
        
        if(msg.type == READ) {
            string read_val = readKey(msg.key);
            if(read_val == "") {
                this->log->logReadFail(&this->memberNode->addr, false, msg.transID, msg.key);
                ack_val = false;
                
            } else {
                ack_val = true;
                Entry read_entry(read_val);
                this->log->logReadSuccess(&this->memberNode->addr, false, msg.transID, msg.key, read_entry.value);
                
                Message read_reply(msg.transID, this->memberNode->addr, read_val);
                string read_reply_string = read_reply.toString();
                
                emulNet->ENsend(&memberNode->addr, &msg.fromAddr, read_reply_string);
            }
            Message ack(msg.transID, this->memberNode->addr, REPLY, ack_val);
            string acktostring = ack.toString();
            
            emulNet->ENsend(&memberNode->addr, &msg.fromAddr, acktostring);
            
        }
        
        if(msg.type == UPDATE){
            if(updateKeyValue(msg.key, msg.value, msg.replica)) {
                this->log->logUpdateSuccess(&this->memberNode->addr, false, msg.transID, msg.key, msg.value);
                ack_val = true;
            }
            else {
                this->log->logUpdateFail(&this->memberNode->addr, false,  msg.transID, msg.key, msg.value);
                ack_val = false;
            }
            Message ack(msg.transID, this->memberNode->addr, REPLY, ack_val);
            string acktostring = ack.toString();
            
            emulNet->ENsend(&memberNode->addr, &msg.fromAddr, acktostring);
        }


        if(msg.type == DELETE) {
            if(deletekey(msg.key)) {
                this->log->logDeleteSuccess(&this->memberNode->addr, false, msg.transID, msg.key);
                ack_val = true;
            } else {
                this->log->logDeleteFail(&this->memberNode->addr, false, msg.transID, msg.key);
                ack_val = false;
            }
            Message ack(msg.transID, this->memberNode->addr, REPLY, ack_val);
            string acktostring = ack.toString();
            
            emulNet->ENsend(&memberNode->addr, &msg.fromAddr, acktostring);

        }
        
        if(msg.type == READREPLY) {
            read_rep_count++;
            map<int, vector<Message>>::iterator itr;
            itr = this->readreplies.find(msg.transID);
            itr->second.push_back(msg);
        }
        
        if(msg.type == REPLY) {
            rep_count++;
            flag = 1;
            //check for quorum before making a decision
            addToQuorum(msg);
            map<int, vector<Message>>::iterator itr_q;
            map<int, pair<int, vector<Message>>>::iterator itr_m;
            map<int, int>::iterator itr_l;
            map<int, vector<Message>>::iterator itr_r;
            itr_l = this->logged.find(msg.transID);
            itr_m = this->quorum.find(msg.transID);
            itr_q = this->queries.find(msg.transID);
            string max_val_read;
            int timestamp_max_read = 0;
            
            vector<Message>::iterator itr_v;
            itr_v = itr_q->second.begin();
            querytype = itr_v->type;
            querykey = itr_v->key;
            if(itr_v->type == READ) {
                itr_r = this->readreplies.find(msg.transID);
                vector<Message>::iterator itr_rv;
                
                //Find the latest value as per the timestamp to be read
                for(itr_rv = itr_r->second.begin(); itr_rv != itr_r->second.end(); ++itr_rv){
                    string read_val = itr_rv->value;
                    Entry entry_read(read_val);
                    if(entry_read.timestamp >= timestamp_max_read){
                        timestamp_max_read = entry_read.timestamp;
                        max_val_read = entry_read.value;
                        
                    }
                }
            }
            
            //If Quorum is achieved and transaction is not logged, log success
            if(itr_m->second.second.size() >= 2 && itr_m->second.first >= 2 && itr_l->second == 0) {
                //success at client
                if(itr_v->type == CREATE){
                    this->log->logCreateSuccess(&this->memberNode->addr, true, itr_v->transID, itr_v->key, itr_v->value);
                }
                if(itr_v->type == DELETE){
                    this->log->logDeleteSuccess(&this->memberNode->addr, true, itr_v->transID, itr_v->key);
                }
                if(itr_v->type == UPDATE){
                    this->log->logUpdateSuccess(&this->memberNode->addr, true, itr_v->transID, itr_v->key, itr_v->value);
                }
                if(itr_v->type == READ){
                    this->log->logReadSuccess(&this->memberNode->addr, true, itr_v->transID, itr_v->key, max_val_read);
                }
                itr_l->second = 1;
                logged = 1;
                
            }
            //If Quorum is achieved but transaction was failure in some replicas, log failure
            else if(itr_m->second.second.size() > 2  && itr_m->second.first < 2 && itr_l->second == 0) {
                //failure at client
                if(itr_v->type == CREATE){
                    this->log->logCreateFail(&this->memberNode->addr, true, itr_v->transID, itr_v->key, itr_v->value);
                }
                if(itr_v->type == DELETE){
                    this->log->logDeleteFail(&this->memberNode->addr, true, itr_v->transID, itr_v->key);
                }
                if(itr_v->type == UPDATE){
                    this->log->logUpdateFail(&this->memberNode->addr, true, itr_v->transID, itr_v->key, itr_v->value);
                }
                if(itr_v->type == READ){
                    this->log->logReadFail(&this->memberNode->addr, true, itr_v->transID, itr_v->key);
                }
                itr_l->second = 1;
                logged = 1;
            }
        }

	}
    
    //If Quorum not achieved, more than 2 replies not obtained, log failure
    if(rep_count < 2 && flag == 1) {
        if(querytype == UPDATE){
            this->log->logUpdateFail(&this->memberNode->addr, true, msgtransID, querykey, msgvalue);
        }
        if(querytype == READ){
            this->log->logReadFail(&this->memberNode->addr, true, msgtransID, querykey);
        }
    }
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
//Function receives the previous and new hasMyReplicas and haveReplicasOf vectors from the updatering function
void MP2Node::stabilizationProtocol(vector<Node> prev_hasMyreplicas, vector<Node> prev_haveReplicasOf, vector<Node> new_hasMyReplicas, vector<Node> new_haveReplicasOf) {
    
    //If Next node has changed circulate the replicas
    if(prev_hasMyreplicas[0].nodeAddress.getAddress() != new_hasMyReplicas[0].nodeAddress.getAddress()){
        map<string,string>::iterator itr;
        
        for(itr=this->ht->hashTable.begin(); itr!=this->ht->hashTable.end(); ++itr){
            Entry e = Entry(itr->second);
            if(e.replica == PRIMARY){
                Message msg(-1, this->memberNode->addr, CREATE, itr->first, e.value, TERTIARY);
                string msgtostring = msg.toString();
                emulNet->ENsend(&this->memberNode->addr, &new_hasMyReplicas[1].nodeAddress, msgtostring);
            }
        }
    }
    
    //If Next-to-Next node has changed
    else if(prev_hasMyreplicas[1].nodeAddress.getAddress() != new_hasMyReplicas[1].nodeAddress.getAddress()) {
        map<string,string>::iterator itr;
        
        for(itr=this->ht->hashTable.begin(); itr!=this->ht->hashTable.end(); ++itr){
            Entry e = Entry(itr->second);
            if(e.replica == PRIMARY){
                Message msg(-1, this->memberNode->addr, CREATE, itr->first, e.value, TERTIARY);
                string msgtostring = msg.toString();
                emulNet->ENsend(&this->memberNode->addr, &new_hasMyReplicas[1].nodeAddress, msgtostring);
            }
        }
    }
    
    //If Previous node has changed
    else if(prev_haveReplicasOf[0].nodeAddress.getAddress() != new_haveReplicasOf[0].nodeAddress.getAddress()){
        map<string,string>::iterator itr;
        
        for(itr=this->ht->hashTable.begin(); itr!=this->ht->hashTable.end(); ++itr){
            Entry e = Entry(itr->second);
            if(e.replica == SECONDARY){
                Message msg(-1, this->memberNode->addr, CREATE, itr->first, e.value, TERTIARY);
                string msgtostring = msg.toString();
                emulNet->ENsend(&this->memberNode->addr, &new_hasMyReplicas[1].nodeAddress, msgtostring);
            }
        }
    }
 }
