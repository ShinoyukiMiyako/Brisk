//! Response bodies of the data plane: the streamed pass-through that frames
//! SSE events and extracts usage without copying chunks, the tap that parses
//! usage from non-streaming responses, and the settlement that runs when a
//! committed response ends, fails or is dropped (R6, R7, R9, R14, R15).
